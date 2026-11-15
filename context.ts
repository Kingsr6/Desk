// Market participants layer: where the big players show up in data a small desk can actually see.
// Everything here is a proxy and is labelled that way: no feed shows central bank or private fund orders.
//  - Central banks      -> FOMC calendar and event blackout (rates data is not connected yet)
//  - Banks / market makers -> order-book spread and depth (passed in from the live feed)
//  - Hedge funds / asset managers -> CFTC weekly positions (CME Bitcoin), top-trader ratio, daily trend (weeks)
//  - Corporations       -> month-end, quarter-end and options-expiry calendar
//  - Retail / crowd     -> funding rate, account long/short ratio, taker flow
const num = (x: unknown) => { const n = Number(x); return Number.isFinite(n) ? n : NaN; };
const FAPI = process.env.BINANCE_FUTURES_URL ?? 'https://fapi.binance.com';
const SPOT = process.env.BINANCE_REST_URL ?? 'https://api.binance.com';
const COT_URL = 'https://publicreporting.cftc.gov/resource/gpe5-46if.json';
const H = 3_600_000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// FOMC decision times (UTC), from the Federal Reserve's published 2026 calendar. Extend with FOMC_EXTRA or EVENT_TIMES.
const FOMC = ['2026-01-28T19:00:00Z', '2026-03-18T18:00:00Z', '2026-04-29T18:00:00Z', '2026-06-17T18:00:00Z', '2026-07-29T18:00:00Z', '2026-09-16T18:00:00Z', '2026-10-28T18:00:00Z', '2026-12-09T19:00:00Z'];

export interface MarketEvent { t: number; name: string }
export function loadEvents(): MarketEvent[] {
  const ev: MarketEvent[] = FOMC.map((s) => ({ t: Date.parse(s), name: 'FOMC rate decision' }));
  for (const s of (process.env.FOMC_EXTRA ?? '').split(',').map((x) => x.trim()).filter(Boolean)) ev.push({ t: Date.parse(s), name: 'FOMC rate decision' });
  // EVENT_TIMES="2026-12-10T13:30:00Z|US CPI,2026-12-04T13:30:00Z|US jobs report"
  for (const item of (process.env.EVENT_TIMES ?? '').split(',').map((x) => x.trim()).filter(Boolean)) {
    const [s, name] = item.split('|');
    if (s) ev.push({ t: Date.parse(s), name: (name ?? 'Event').trim() });
  }
  return ev.filter((e) => Number.isFinite(e.t)).sort((a, b) => a.t - b.t);
}

export function lastFridayExpiry(now: number): number {
  const d = new Date(now);
  for (let k = 0; k < 2; k++) {
    const y = d.getUTCFullYear(), m = d.getUTCMonth() + k;
    const last = new Date(Date.UTC(y, m + 1, 0, 8, 0, 0));
    last.setUTCDate(last.getUTCDate() - ((last.getUTCDay() + 2) % 7)); // back to Friday
    if (last.getTime() > now) return last.getTime();
  }
  return now;
}
const monthEnd = (now: number) => { const d = new Date(now); return Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1); };
const quarterEnd = (now: number) => { const d = new Date(now); return Date.UTC(d.getUTCFullYear(), (Math.floor(d.getUTCMonth() / 3) + 1) * 3, 1); };

export interface Crowd { fundingPct8h?: number; oiChange24hPct?: number; topTraderLS?: number; accountLS?: number; takerBuySell?: number; crowding: 'long-crowded' | 'short-crowded' | 'balanced' | 'unknown' }
export interface Trend { bias: 'up' | 'down' | 'mixed'; close: number; sma50: number; sma200: number; ret30dPct: number }
export interface Cot { market: string; reportDate: string; levNet: number; levNetChange: number; levNetPctOI: number; assetMgrNet: number; dealerNet: number; ageDays: number }

export function classifyCrowd(c: Partial<Crowd>): Crowd['crowding'] {
  const f = c.fundingPct8h;
  if (f === undefined || Number.isNaN(f)) return 'unknown';
  if (f >= 0.03 || ((c.accountLS ?? 0) > 2.2 && f > 0.015)) return 'long-crowded';
  if (f <= -0.02 || ((c.accountLS ?? 9) < 0.8 && f < -0.01)) return 'short-crowded';
  return 'balanced';
}

export class MarketContext {
  private events = loadEvents();
  private crowd = new Map<string, Crowd>();
  private trend = new Map<string, Trend>();
  private cot: Cot | null = null;
  private errors = new Map<string, string>();

  constructor(private symbols: string[]) {}

  start() {
    const run = (name: string, fn: () => Promise<void>) => fn().then(() => this.errors.delete(name)).catch((e) => { this.errors.set(name, (e as Error).message); console.warn(`context ${name}:`, (e as Error).message); });
    const crowd = () => run('crowd', () => this.refreshCrowd());
    const trend = () => run('trend', () => this.refreshTrend());
    const cot = () => run('cot', () => this.refreshCot());
    void trend(); void crowd(); void cot();
    setInterval(() => void crowd(), 5 * 60_000);
    setInterval(() => void trend(), 30 * 60_000);
    setInterval(() => void cot(), 6 * H);
  }

  private async json(url: string): Promise<any> {
    const r = await fetch(url, { signal: AbortSignal.timeout(10_000), headers: { accept: 'application/json' } });
    if (!r.ok) throw new Error(`HTTP ${r.status} from ${new URL(url).host}`);
    return r.json();
  }

  async refreshCrowd() {
    let failure = '';
    for (const s of this.symbols) {
      const c: Partial<Crowd> = {};
      const part = async (fn: () => Promise<void>) => { try { await fn(); } catch (e) { failure = (e as Error).message; } await sleep(250); };
      await part(async () => { c.fundingPct8h = num((await this.json(`${FAPI}/fapi/v1/premiumIndex?symbol=${s}`)).lastFundingRate) * 100; });
      await part(async () => { const h = await this.json(`${FAPI}/futures/data/openInterestHist?symbol=${s}&period=1h&limit=25`); c.oiChange24hPct = (num(h.at(-1).sumOpenInterestValue) / num(h[0].sumOpenInterestValue) - 1) * 100; });
      await part(async () => { c.topTraderLS = num((await this.json(`${FAPI}/futures/data/topLongShortPositionRatio?symbol=${s}&period=1h&limit=1`))[0].longShortRatio); });
      await part(async () => { c.accountLS = num((await this.json(`${FAPI}/futures/data/globalLongShortAccountRatio?symbol=${s}&period=1h&limit=1`))[0].longShortRatio); });
      await part(async () => { c.takerBuySell = num((await this.json(`${FAPI}/futures/data/takerlongshortRatio?symbol=${s}&period=1h&limit=1`))[0].buySellRatio); });
      this.crowd.set(s, { ...c, crowding: classifyCrowd(c) });
    }
    if (failure) throw new Error(failure);
  }

  async refreshTrend() {
    for (const s of this.symbols) {
      const rows = await this.json(`${SPOT}/api/v3/klines?symbol=${s}&interval=1d&limit=250`);
      const closes: number[] = rows.map((k: any[]) => Number(k[4]));
      if (closes.length < 200) continue;
      const sma = (n: number) => closes.slice(-n).reduce((a, b) => a + b, 0) / n;
      const close = closes[closes.length - 1]!, s50 = sma(50), s200 = sma(200);
      this.trend.set(s, { bias: close > s50 && s50 > s200 ? 'up' : close < s50 && s50 < s200 ? 'down' : 'mixed', close, sma50: s50, sma200: s200, ret30dPct: (close / closes[closes.length - 31]! - 1) * 100 });
      await sleep(250);
    }
  }

  async refreshCot() {
    if (!this.symbols.includes('BTCUSDT')) return;
    const q = new URLSearchParams({ $where: "upper(market_and_exchange_names) like 'BITCOIN - CHICAGO%'", $order: 'report_date_as_yyyy_mm_dd DESC', $limit: '2' });
    const rows = await this.json(`${COT_URL}?${q}`);
    if (!Array.isArray(rows) || !rows[0]) throw new Error('CFTC returned no Bitcoin rows');
    const net = (r: any, p: string) => num(r[`${p}_positions_long_all`]) - num(r[`${p}_positions_short_all`]);
    const r0 = rows[0], r1 = rows[1];
    const oi = num(r0.open_interest_all);
    this.cot = {
      market: String(r0.market_and_exchange_names), reportDate: String(r0.report_date_as_yyyy_mm_dd).slice(0, 10),
      levNet: net(r0, 'lev_money'), levNetChange: r1 ? net(r0, 'lev_money') - net(r1, 'lev_money') : NaN,
      levNetPctOI: (net(r0, 'lev_money') / oi) * 100, assetMgrNet: net(r0, 'asset_mgr'), dealerNet: net(r0, 'dealer'),
      ageDays: (Date.now() - Date.parse(String(r0.report_date_as_yyyy_mm_dd))) / 86_400_000,
    };
  }

  nextEvent(now = Date.now()) { return this.events.find((e) => e.t > now - 3 * H) ?? null; }

  /** No new positions from 2 hours before to 3 hours after a scheduled central bank or macro event. */
  eventBlackout(now = Date.now()): { active: boolean; reason: string } {
    const e = this.events.find((x) => now >= x.t - 2 * H && now <= x.t + 3 * H);
    return e ? { active: true, reason: `event blackout around ${e.name} (${new Date(e.t).toISOString().slice(0, 16)}Z)` } : { active: false, reason: '' };
  }

  /** Compact, factual context handed to the Claude analyst. */
  forSymbol(symbol: string, now = Date.now()) {
    const ev = this.nextEvent(now);
    return {
      macro: {
        next_event: ev?.name ?? 'none scheduled in the calendar', hours_to_next_event: ev ? Math.round((ev.t - now) / H) : null,
        blackout_active: this.eventBlackout(now).active,
        days_to_month_end: Math.round((monthEnd(now) - now) / 86_400_000), days_to_quarter_end: Math.round((quarterEnd(now) - now) / 86_400_000),
        hours_to_monthly_options_expiry: Math.round((lastFridayExpiry(now) - now) / H),
      },
      daily_trend_weeks_horizon: this.trend.get(symbol) ?? 'unavailable',
      crowd_positioning: this.crowd.get(symbol) ?? 'unavailable',
      institutions_weekly_lagged: symbol === 'BTCUSDT' ? (this.cot ?? 'unavailable') : 'not tracked for this symbol',
      caution: 'Leveraged-fund shorts in CME Bitcoin futures are often basis-trade hedges, not bearish bets. Crowd data covers Binance only.',
    };
  }

  snapshot(micro: Record<string, { spreadBps: number; bookImb: number }>, now = Date.now()) {
    const f = (x: number | undefined, d = 2) => (x === undefined || !Number.isFinite(x) ? 'n/a' : x.toFixed(d));
    const ev = this.nextEvent(now), bl = this.eventBlackout(now);
    const short = (s: string) => s.replace('USDT', '');
    const tiers = [
      { name: 'Central banks', role: 'Set policy and long-term direction', lines: [
        ev ? `Next: ${ev.name} at ${new Date(ev.t).toISOString().slice(0, 16).replace('T', ' ')} UTC (in ${((ev.t - now) / 86_400_000).toFixed(1)} days)` : 'No upcoming event in the calendar. Add dates with FOMC_EXTRA or EVENT_TIMES.',
        bl.active ? `Blackout active: ${bl.reason}. No new plans are approved.` : 'No blackout now. New plans pause 2 hours before to 3 hours after events.',
        'Not connected yet: rate expectations, yields and the dollar index.' ] },
      { name: 'Banks and market makers', role: 'Make markets, absorb and distribute positions', lines:
        this.symbols.map((s) => `${short(s)}: spread ${f(micro[s]?.spreadBps, 2)} bps, book imbalance ${f(micro[s]?.bookImb)}`).concat('Visible only as order-book liquidity. Their client flow is private.') },
      { name: 'Hedge funds and asset managers', role: 'Large directional bets held for weeks or months', lines: [
        this.cot ? `CME Bitcoin (weekly, ${f(this.cot.ageDays, 0)} days old): leveraged funds net ${Math.round(this.cot.levNet).toLocaleString('en-US')} contracts (${f(this.cot.levNetPctOI, 1)}% of open interest), change ${f(this.cot.levNetChange, 0)}; asset managers net ${Math.round(this.cot.assetMgrNet).toLocaleString('en-US')}` : 'CFTC positions unavailable.',
        ...this.symbols.map((s) => { const t = this.trend.get(s), c = this.crowd.get(s); return `${short(s)}: daily trend ${t ? `${t.bias} (30d ${f(t.ret30dPct, 1)}%)` : 'n/a'}, top-trader long/short ${f(c?.topTraderLS)}`; }),
        'Horizon: weeks. The desk trades minutes, so this layer is context and a filter, not a trigger. Fund shorts in CME Bitcoin are often basis-trade hedges.' ] },
      { name: 'Corporations and scheduled flows', role: 'Predictable flows at month and quarter end', lines: [
        `Month end in ${((monthEnd(now) - now) / 86_400_000).toFixed(1)} days, quarter end in ${((quarterEnd(now) - now) / 86_400_000).toFixed(0)} days`,
        `Monthly options and futures expiry in ${((lastFridayExpiry(now) - now) / H).toFixed(0)} hours (last Friday, 08:00 UTC)` ] },
      { name: 'Retail and crowd', role: 'Follows the flow, provides liquidity to larger players', lines:
        this.symbols.map((s) => { const c = this.crowd.get(s); return `${short(s)}: funding ${f(c?.fundingPct8h, 3)}%/8h, OI 24h ${f(c?.oiChange24hPct, 1)}%, accounts long/short ${f(c?.accountLS)}, taker buy/sell ${f(c?.takerBuySell)}, ${c?.crowding ?? 'unknown'}`; }) },
    ];
    return { asOf: now, tiers, errors: [...this.errors].map(([k, v]) => `${k}: ${v}`) };
  }
}
