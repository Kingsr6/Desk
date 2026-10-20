// Degen radar: PAPER ONLY. Watches new tokens on DexScreener, filters obvious traps,
// simulates entries and exits with fees and price impact. It never touches a wallet or an exchange.
const API = 'https://api.dexscreener.com';
const num = (x: unknown, d = 0) => { const n = Number(x); return Number.isFinite(n) ? n : d; };
const env = (k: string, d: number) => num(process.env[k], d);
const clamp = (x: number, a = -1, b = 1) => Math.max(a, Math.min(b, x));

export const CFG = {
  chains: (process.env.RADAR_CHAINS ?? 'solana').split(',').map((s) => s.trim()).filter(Boolean),
  bankroll: env('RADAR_BANKROLL', 100),      // paper USD
  posFraction: env('RADAR_POS_FRACTION', 0.05),
  maxPositions: env('RADAR_MAX_POSITIONS', 5),
  maxTracked: 300,
  minLiq: env('RADAR_MIN_LIQ', 15000),
  minAgeMin: 15, maxAgeHours: 12,
  minTxns1h: 100, minSells1h: 15,
  maxPump1h: 400, maxCapToLiq: 60,
  enterScore: env('RADAR_ENTER_SCORE', 0.35),
  fee: 0.01,          // 1% per side (pool fee plus platform fee, rough)
  netCost: 0.03,      // USD per transaction (network and priority fees)
  stopLoss: -0.2, takeProfit: 0.5, maxHoldMin: 60,
};

interface Pair {
  chainId: string; dexId?: string; pairAddress?: string;
  baseToken: { address: string; symbol: string; name?: string };
  priceUsd?: string; fdv?: number; marketCap?: number; pairCreatedAt?: number;
  txns?: Record<string, { buys: number; sells: number }>;
  volume?: Record<string, number>;
  priceChange?: Record<string, number>;
  liquidity?: { usd?: number };
}
interface Eval { ok: boolean; score: number; reject?: string }
interface Tok { key: string; chain: string; address: string; firstSeen: number; lastSeen: number; pair?: Pair; traded: boolean; eval?: Eval }
interface Pos { id: number; key: string; symbol: string; chain: string; cost: number; units: number; entryPrice: number; entryLiq: number; openedAt: number; score: number; mark: number; value: number; pnlPct: number }
interface Closed { symbol: string; reason: string; cost: number; proceeds: number; pnl: number; pnlPct: number; heldMin: number; closedAt: number }

export function evaluate(p: Pair, now: number): Eval {
  const bad = (reject: string): Eval => ({ ok: false, score: 0, reject });
  const price = num(p.priceUsd);
  if (!price) return bad('no price');
  const liq = num(p.liquidity?.usd);
  if (liq < CFG.minLiq) return bad('liquidity too low');
  if (!p.pairCreatedAt) return bad('unknown age');
  const ageMin = (now - p.pairCreatedAt) / 60_000;
  if (ageMin < CFG.minAgeMin) return bad('too new');
  if (ageMin > CFG.maxAgeHours * 60) return bad('too old');
  const buys = num(p.txns?.h1?.buys), sells = num(p.txns?.h1?.sells);
  if (buys + sells < CFG.minTxns1h) return bad('low activity');
  if (sells < CFG.minSells1h) return bad('few sells (honeypot risk)');
  const buyRatio = buys / (buys + sells);
  if (buyRatio > 0.8) return bad('buy-only skew (honeypot or bot risk)');
  if (buyRatio < 0.45) return bad('sell-dominated');
  const ch5 = num(p.priceChange?.m5), ch1 = num(p.priceChange?.h1);
  if (ch1 > CFG.maxPump1h) return bad('already pumped');
  const cap = num(p.marketCap ?? p.fdv);
  if (cap / liq > CFG.maxCapToLiq) return bad('market cap far above liquidity');
  if (ch5 <= 0) return bad('momentum not positive');
  const v5 = num(p.volume?.m5), v1 = num(p.volume?.h1);
  const accel = Math.tanh((v5 * 12) / Math.max(v1, 1) - 1);
  const score = 0.4 * Math.tanh(ch5 / 10) + 0.3 * accel + 0.3 * clamp((buyRatio - 0.5) * 4);
  return { ok: true, score };
}

/** Constant-product style estimate: impact is about 2 x trade size / total pool liquidity. */
const impact = (usd: number, liq: number) => Math.min(0.9, (2 * usd) / Math.max(liq, 1));
export function buyUnits(size: number, price: number, liq: number) {
  const eff = price * (1 + impact(size, liq));
  return Math.max(0, size - size * CFG.fee - CFG.netCost) / eff;
}
export function sellProceeds(units: number, price: number, liq: number) {
  const gross = units * price;
  return Math.max(0, gross * (1 - impact(gross, liq)) * (1 - CFG.fee) - CFG.netCost);
}

export class Radar {
  paused = false;
  apiOk = false;
  private toks = new Map<string, Tok>();
  private cash = CFG.bankroll;
  private open: Pos[] = [];
  private closed: Closed[] = [];
  private rejects: Record<string, number> = {};
  private peak = CFG.bankroll;
  private maxDD = 0;
  private pausedUntil = 0;
  private tick = 0;
  private seq = 0;
  private lastError = '';

  constructor(private record: (type: string, payload: unknown) => void) {}

  start() {
    const loop = async () => {
      try { await this.cycle(); } catch (e) { this.lastError = (e as Error).message; console.error('radar:', this.lastError); }
      setTimeout(loop, 10_000);
    };
    void loop();
  }

  async cycle() {
    if (Date.now() < this.pausedUntil) return;
    this.tick++;
    if (this.tick % 2 === 1) await this.discover();
    await this.refresh();
    this.manage();
    this.scan();
    this.trackEquity();
  }

  private async get(path: string): Promise<any> {
    const r = await fetch(API + path, { signal: AbortSignal.timeout(10_000), headers: { accept: 'application/json' } });
    if (r.status === 429) { this.pausedUntil = Date.now() + 30_000; this.apiOk = false; throw new Error('rate limited, pausing 30s'); }
    if (!r.ok) { this.apiOk = false; throw new Error(`DexScreener HTTP ${r.status}`); }
    this.apiOk = true;
    return r.json();
  }

  private async discover() {
    for (const path of ['/token-profiles/latest/v1', '/token-boosts/latest/v1']) {
      const arr = await this.get(path);
      if (!Array.isArray(arr)) continue;
      for (const it of arr) {
        if (!CFG.chains.includes(it?.chainId) || !it?.tokenAddress) continue;
        const key = `${it.chainId}:${it.tokenAddress}`;
        if (!this.toks.has(key)) {
          this.toks.set(key, { key, chain: it.chainId, address: it.tokenAddress, firstSeen: Date.now(), lastSeen: 0, traded: false });
        }
      }
    }
    if (this.toks.size > CFG.maxTracked) {
      const held = new Set(this.open.map((p) => p.key));
      const victims = [...this.toks.values()].filter((t) => !held.has(t.key)).sort((a, b) => a.firstSeen - b.firstSeen);
      for (const v of victims.slice(0, this.toks.size - CFG.maxTracked)) this.toks.delete(v.key);
    }
  }

  private async refresh() {
    const byAddr = new Map<string, Tok>();
    for (const t of this.toks.values()) byAddr.set(t.address, t);
    const addrs = [...byAddr.keys()];
    for (let i = 0; i < addrs.length; i += 30) {
      const data = await this.get(`/latest/dex/tokens/${addrs.slice(i, i + 30).join(',')}`);
      const best = new Map<string, Pair>();
      for (const p of (data?.pairs ?? []) as Pair[]) {
        const a = p.baseToken?.address;
        const tok = a ? byAddr.get(a) : undefined;
        if (!tok || tok.chain !== p.chainId) continue;
        const cur = best.get(a!);
        if (!cur || num(p.liquidity?.usd) > num(cur.liquidity?.usd)) best.set(a!, p);
      }
      for (const [a, p] of best) { const t = byAddr.get(a)!; t.pair = p; t.lastSeen = Date.now(); }
    }
    // free slots held by tokens that aged out (never drop open positions)
    const now = Date.now(), held = new Set(this.open.map((p) => p.key));
    for (const t of this.toks.values()) {
      const age = t.pair?.pairCreatedAt ? (now - t.pair.pairCreatedAt) / 3_600_000 : 0;
      if (!held.has(t.key) && age > CFG.maxAgeHours * 2) this.toks.delete(t.key);
    }
  }

  private equity() { return this.cash + this.open.reduce((s, p) => s + p.value, 0); }

  private manage() {
    const now = Date.now();
    for (const pos of [...this.open]) {
      const tok = this.toks.get(pos.key), p = tok?.pair;
      const stale = !tok || now - tok.lastSeen > 10 * 60_000;
      let reason = '', proceeds = 0;
      if (p && !stale) {
        const price = num(p.priceUsd), liq = num(p.liquidity?.usd);
        pos.mark = price;
        pos.value = sellProceeds(pos.units, price, liq);
        pos.pnlPct = pos.value / pos.cost - 1;
        proceeds = pos.value;
        if (liq < 1000) { reason = 'rug: liquidity gone'; proceeds = 0; }
        else if (liq < 0.6 * pos.entryLiq) reason = 'liquidity dropped 40%';
        else if (pos.pnlPct <= CFG.stopLoss) reason = 'stop loss';
        else if (pos.pnlPct >= CFG.takeProfit) reason = 'take profit';
        else if (now - pos.openedAt > CFG.maxHoldMin * 60_000) reason = 'time limit';
      } else if (stale) { reason = 'lost data (50% haircut)'; proceeds = pos.value * 0.5; }
      if (!reason) continue;
      this.open = this.open.filter((x) => x !== pos);
      this.cash += proceeds;
      const c: Closed = { symbol: pos.symbol, reason, cost: pos.cost, proceeds, pnl: proceeds - pos.cost, pnlPct: proceeds / pos.cost - 1, heldMin: (now - pos.openedAt) / 60_000, closedAt: now };
      this.closed.unshift(c); if (this.closed.length > 100) this.closed.pop();
      this.record('radar_exit', c);
    }
  }

  private scan() {
    const now = Date.now(), counts: Record<string, number> = {}, picks: { tok: Tok; score: number }[] = [];
    for (const tok of this.toks.values()) {
      if (!tok.pair) continue;
      const ev = evaluate(tok.pair, now);
      tok.eval = ev;
      if (!ev.ok) { counts[ev.reject!] = (counts[ev.reject!] ?? 0) + 1; continue; }
      if (!tok.traded && ev.score >= CFG.enterScore) picks.push({ tok, score: ev.score });
    }
    this.rejects = counts;
    if (this.paused) return;
    picks.sort((a, b) => b.score - a.score);
    for (const { tok, score } of picks) {
      if (this.open.length >= CFG.maxPositions) break;
      const size = Math.min(this.cash, this.equity() * CFG.posFraction);
      if (size < 1) break;
      const p = tok.pair!, price = num(p.priceUsd), liq = num(p.liquidity?.usd);
      const units = buyUnits(size, price, liq);
      this.cash -= size; tok.traded = true;
      const pos: Pos = { id: ++this.seq, key: tok.key, symbol: p.baseToken.symbol, chain: tok.chain, cost: size, units, entryPrice: price, entryLiq: liq, openedAt: now, score, mark: price, value: sellProceeds(units, price, liq), pnlPct: 0 };
      pos.pnlPct = pos.value / pos.cost - 1;
      this.open.push(pos);
      this.record('radar_entry', { symbol: pos.symbol, chain: tok.chain, address: tok.address, price, liq, score, size });
    }
  }

  private trackEquity() {
    const eq = this.equity();
    this.peak = Math.max(this.peak, eq);
    this.maxDD = Math.max(this.maxDD, (this.peak - eq) / this.peak);
  }

  snapshot() {
    const now = Date.now(), c = this.closed, wins = c.filter((x) => x.pnl > 0).length;
    const watch = [...this.toks.values()].filter((t) => t.eval?.ok && t.pair).sort((a, b) => b.eval!.score - a.eval!.score).slice(0, 8)
      .map((t) => ({ symbol: t.pair!.baseToken.symbol, score: t.eval!.score, liq: num(t.pair!.liquidity?.usd), m5: num(t.pair!.priceChange?.m5), h1: num(t.pair!.priceChange?.h1), ageMin: (now - num(t.pair!.pairCreatedAt)) / 60_000, traded: t.traded }));
    return {
      apiOk: this.apiOk, paused: this.paused, tracked: this.toks.size, error: this.lastError,
      start: CFG.bankroll, cash: this.cash, equity: this.equity(), maxDD: this.maxDD,
      trades: c.length, wins, avgPct: c.length ? c.reduce((s, x) => s + x.pnlPct, 0) / c.length : 0,
      open: this.open.map((p) => ({ symbol: p.symbol, cost: p.cost, value: p.value, pnlPct: p.pnlPct, heldMin: (now - p.openedAt) / 60_000 })),
      closed: c.slice(0, 12), rejects: this.rejects, watch,
      cfg: { minLiq: CFG.minLiq, tp: CFG.takeProfit, sl: CFG.stopLoss, fee: CFG.fee, chains: CFG.chains },
    };
  }
}
