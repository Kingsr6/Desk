// Executor: places orders on the Binance SPOT TESTNET only (fake money).
// Live trading is deliberately not built. Spot cannot short, so only long plans are executed.
// Every entry is followed immediately by an exchange-side OCO (target + stop). If that fails, the position is flattened.
import { createHmac } from 'node:crypto';

const BASE = 'https://testnet.binance.vision';
const num = (x: unknown, d = 0) => { const n = Number(x); return Number.isFinite(n) ? n : d; };
const decimals = (x: number) => { const s = String(x); if (s.includes('e-')) return Number(s.split('e-')[1]); const i = s.indexOf('.'); return i < 0 ? 0 : s.length - i - 1; };
const floorTo = (v: number, step: number) => (Math.floor(v / step + 1e-9) * step).toFixed(decimals(step));
const roundTo = (v: number, tick: number) => (Math.round(v / tick) * tick).toFixed(decimals(tick));
const cid = (s: string) => s.replace(/[^A-Za-z0-9_-]/g, '-').slice(0, 36);

export interface PlanLike { planId: string; symbol: string; action: string; entry: number; stop: number; target: number }
interface Position { planId: string; symbol: string; qty: number; entry: number; entryQuote: number; stop: number; target: number; orderListId: number; openedAt: number }
interface ClosedTrade { symbol: string; reason: string; entry: number; exit: number; qty: number; pnl: number; closedAt: number }
interface SymInfo { step: number; tick: number; minNotional: number; base: string }

export class Executor {
  readonly key = process.env.BINANCE_TESTNET_KEY ?? '';
  readonly secret = process.env.BINANCE_TESTNET_SECRET ?? '';
  enabled = !!(this.key && this.secret);
  positions: Position[] = [];
  closed: ClosedTrade[] = [];
  lastError = '';
  private info = new Map<string, SymInfo>();
  private busy = false;

  constructor(private record: (type: string, payload: unknown) => void) {}

  start() {
    if (!this.enabled) return console.warn('executor off: set BINANCE_TESTNET_KEY and BINANCE_TESTNET_SECRET to enable testnet orders');
    console.log('executor on (Binance spot testnet only)');
    setInterval(() => void this.reconcile(), 30_000);
  }

  private async call(method: 'GET' | 'POST' | 'DELETE', path: string, params: Record<string, string | number> = {}, signed = true): Promise<any> {
    const q = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) q.set(k, String(v));
    if (signed) {
      q.set('timestamp', String(Date.now()));
      q.set('recvWindow', '5000');
      q.set('signature', createHmac('sha256', this.secret).update(q.toString()).digest('hex'));
    }
    const r = await fetch(`${BASE}${path}?${q}`, { method, headers: { 'X-MBX-APIKEY': this.key }, signal: AbortSignal.timeout(10_000) });
    const body: any = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`Binance ${r.status} ${body?.code ?? ''} ${body?.msg ?? ''}`.trim());
    return body;
  }

  private async symInfo(symbol: string): Promise<SymInfo> {
    const hit = this.info.get(symbol);
    if (hit) return hit;
    const d = await this.call('GET', '/api/v3/exchangeInfo', { symbol }, false);
    const s = d.symbols?.[0];
    const f = (t: string) => s?.filters?.find((x: any) => x.filterType === t);
    const info: SymInfo = {
      step: num(f('LOT_SIZE')?.stepSize, 0.00001), tick: num(f('PRICE_FILTER')?.tickSize, 0.01),
      minNotional: num((f('NOTIONAL') ?? f('MIN_NOTIONAL'))?.minNotional, 5), base: String(s?.baseAsset ?? ''),
    };
    this.info.set(symbol, info);
    return info;
  }

  realizedToday() {
    const start = new Date(); start.setUTCHours(0, 0, 0, 0);
    return this.closed.filter((c) => c.closedAt >= start.getTime()).reduce((s, c) => s + c.pnl, 0);
  }

  async execute(plan: PlanLike, wantedQty: number): Promise<{ ok: boolean; message: string }> {
    const fail = (message: string) => { this.lastError = message; return { ok: false, message: `Not executed: ${message}` }; };
    if (!this.enabled) return fail('no testnet keys');
    if (plan.action !== 'long') return fail('spot cannot short');
    if (this.busy) return fail('another order is in progress');
    this.busy = true;
    try {
      const info = await this.symInfo(plan.symbol);
      const last = num((await this.call('GET', '/api/v3/ticker/price', { symbol: plan.symbol }, false)).price);
      if (!last || Math.abs(last - plan.entry) / plan.entry > 0.005) return fail('price moved more than 0.5% since the plan');
      if (!(plan.stop < last && plan.target > last)) return fail('stop or target is on the wrong side of the current price');
      const qty = floorTo(wantedQty, info.step);
      if (num(qty) * last < info.minNotional) return fail(`order below the minimum size (${info.minNotional} USDT)`);
      const clientId = cid(`d-${plan.planId}`);

      const o = await this.call('POST', '/api/v3/order', { symbol: plan.symbol, side: 'BUY', type: 'MARKET', quantity: qty, newClientOrderId: clientId, newOrderRespType: 'FULL' });
      const executed = num(o.executedQty), quote = num(o.cummulativeQuoteQty);
      if (!executed) return fail('entry order did not fill');
      const baseFee = (o.fills ?? []).filter((f: any) => f.commissionAsset === info.base).reduce((s: number, f: any) => s + num(f.commission), 0);
      const sellQty = floorTo(executed - baseFee, info.step);
      this.record('exec_entry', { planId: plan.planId, symbol: plan.symbol, qty: executed, avg: quote / executed });

      try {
        const oco = await this.call('POST', '/api/v3/orderList/oco', {
          symbol: plan.symbol, side: 'SELL', quantity: sellQty,
          aboveType: 'LIMIT_MAKER', abovePrice: roundTo(plan.target, info.tick),
          belowType: 'STOP_LOSS_LIMIT', belowStopPrice: roundTo(plan.stop, info.tick), belowPrice: roundTo(plan.stop * 0.998, info.tick), belowTimeInForce: 'GTC',
          listClientOrderId: cid(`o-${clientId}`),
        });
        this.positions.push({ planId: plan.planId, symbol: plan.symbol, qty: num(sellQty), entry: quote / executed, entryQuote: quote, stop: plan.stop, target: plan.target, orderListId: num(oco.orderListId), openedAt: Date.now() });
        this.lastError = '';
        return { ok: true, message: `Filled ${executed} ${info.base} at ${(quote / executed).toFixed(2)} with protective stop and target on the exchange` };
      } catch (e) {
        // fail safe: never leave a position without exchange-side protection
        const msg = (e as Error).message;
        try {
          const s = await this.call('POST', '/api/v3/order', { symbol: plan.symbol, side: 'SELL', type: 'MARKET', quantity: sellQty, newOrderRespType: 'FULL' });
          this.closed.unshift({ symbol: plan.symbol, reason: 'protection failed, flattened', entry: quote / executed, exit: num(s.cummulativeQuoteQty) / Math.max(num(s.executedQty), 1e-12), qty: num(sellQty), pnl: num(s.cummulativeQuoteQty) - quote, closedAt: Date.now() });
          return fail(`protective order failed (${msg}); position flattened`);
        } catch (e2) {
          this.lastError = `UNPROTECTED position in ${plan.symbol}: ${msg}; flatten failed: ${(e2 as Error).message}`;
          this.record('exec_alert', { symbol: plan.symbol, error: this.lastError });
          return { ok: false, message: `ALERT: ${this.lastError}` };
        }
      }
    } catch (e) {
      return fail((e as Error).message);
    } finally {
      this.busy = false;
    }
  }

  private finish(pos: Position, reason: string, exitQuote: number) {
    this.positions = this.positions.filter((p) => p !== pos);
    const t: ClosedTrade = { symbol: pos.symbol, reason, entry: pos.entry, exit: exitQuote / pos.qty, qty: pos.qty, pnl: exitQuote - pos.entryQuote, closedAt: Date.now() };
    this.closed.unshift(t);
    if (this.closed.length > 100) this.closed.pop();
    this.record('exec_close', t);
  }

  /** Compare local positions with the exchange: detect target/stop fills. */
  async reconcile() {
    if (!this.enabled || !this.positions.length) return;
    for (const pos of [...this.positions]) {
      try {
        const list = await this.call('GET', '/api/v3/orderList', { orderListId: pos.orderListId });
        if (list.listOrderStatus === 'EXECUTING') continue;
        for (const leg of list.orders ?? []) {
          const o = await this.call('GET', '/api/v3/order', { symbol: pos.symbol, orderId: leg.orderId });
          if (o.status === 'FILLED') {
            this.finish(pos, o.type === 'LIMIT_MAKER' ? 'target hit' : 'stop hit', num(o.cummulativeQuoteQty));
            break;
          }
        }
      } catch (e) {
        this.lastError = `reconcile ${pos.symbol}: ${(e as Error).message}`;
      }
    }
  }

  /** Cancel protective orders and sell everything at market. */
  async flatten() {
    for (const pos of [...this.positions]) {
      try {
        try { await this.call('DELETE', '/api/v3/orderList', { symbol: pos.symbol, orderListId: pos.orderListId }); } catch { /* may already be filled */ }
        const info = await this.symInfo(pos.symbol);
        const s = await this.call('POST', '/api/v3/order', { symbol: pos.symbol, side: 'SELL', type: 'MARKET', quantity: floorTo(pos.qty, info.step), newOrderRespType: 'FULL' });
        this.finish(pos, 'flattened by you', num(s.cummulativeQuoteQty));
      } catch (e) {
        this.lastError = `flatten ${pos.symbol}: ${(e as Error).message}`;
      }
    }
  }

  snapshot() {
    const total = this.closed.reduce((s, c) => s + c.pnl, 0);
    return {
      enabled: this.enabled, venue: 'Binance spot testnet', error: this.lastError, realizedToday: this.realizedToday(), total,
      positions: this.positions.map((p) => ({ symbol: p.symbol, qty: p.qty, entry: p.entry, stop: p.stop, target: p.target, heldMin: (Date.now() - p.openedAt) / 60_000 })),
      closed: this.closed.slice(0, 10),
    };
  }
}
