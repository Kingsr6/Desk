// Desk milestone 1: live Binance market data -> signals -> WebSocket UI. No orders are placed.
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { EventEmitter } from 'node:events';
import WebSocket, { WebSocketServer } from 'ws';

// ---------- types ----------
interface Candle { t: number; o: number; h: number; l: number; c: number; v: number }
type Level = [price: number, qty: number];
interface Book { bids: Level[]; asks: Level[] }
/** Every signal is normalized to the range -1..+1. */
interface Signals { momentum: number; volume: number; volatility: number; book: number }
interface SymbolSnapshot {
  symbol: string;
  price: number;
  signals: Signals;
  composite: number;
  candles: number;
  stale: boolean;
}
interface Snapshot { type: 'snapshot'; ts: number; connected: boolean; symbols: SymbolSnapshot[] }

// ---------- signals ----------

const clamp = (x: number, a = -1, b = 1) => Math.max(a, Math.min(b, x));
const mean = (a: number[]) => (a.length ? a.reduce((s, x) => s + x, 0) / a.length : 0);
const sd = (a: number[]) => {
  const m = mean(a);
  return Math.sqrt(mean(a.map((x) => (x - m) ** 2)));
};
const logReturns = (c: number[]) => c.slice(1).map((x, i) => Math.log(x / (c[i] as number)));

/** Return over `window` candles divided by recent volatility, squashed with tanh. */
function momentum(candles: Candle[], window = 15): number {
  const closes = candles.map((c) => c.c);
  if (closes.length < window + 20) return 0;
  const s = sd(logReturns(closes.slice(-60)));
  if (s === 0) return 0;
  const last = closes[closes.length - 1] as number;
  const past = closes[closes.length - 1 - window] as number;
  return clamp(Math.tanh(Math.log(last / past) / (s * Math.sqrt(window)) / 2));
}

/** Recent volume against its baseline, signed by candle direction. */
function volumePressure(candles: Candle[]): number {
  if (candles.length < 30) return 0;
  const base = candles.slice(-100, -5).map((c) => c.v);
  const recent = candles.slice(-5);
  const s = sd(base);
  if (s === 0) return 0;
  const z = (mean(recent.map((c) => c.v)) - mean(base)) / s;
  const dir = Math.sign(recent.reduce((a, c) => a + (c.c - c.o), 0));
  return clamp(Math.tanh(z / 2) * dir);
}

/** Short-term volatility versus longer-term. Positive = expanding, negative = calming. */
function volatilityRegime(candles: Candle[]): number {
  const closes = candles.map((c) => c.c);
  if (closes.length < 60) return 0;
  const long = sd(logReturns(closes.slice(-100)));
  if (long === 0) return 0;
  const short = sd(logReturns(closes.slice(-30)));
  return clamp(short / long - 1);
}

/** (bid depth - ask depth) / total, within `band` of mid price. */
function bookImbalance(book: Book, band = 0.005): number {
  const bb = book.bids[0];
  const ba = book.asks[0];
  if (!bb || !ba) return 0;
  const mid = (bb[0] + ba[0]) / 2;
  const bids = book.bids.filter(([p]) => p >= mid * (1 - band)).reduce((s, [, q]) => s + q, 0);
  const asks = book.asks.filter(([p]) => p <= mid * (1 + band)).reduce((s, [, q]) => s + q, 0);
  const total = bids + asks;
  return total === 0 ? 0 : clamp((bids - asks) / total);
}

const WEIGHTS = { momentum: 0.4, volume: 0.25, book: 0.35 } as const;

function computeSignals(candles: Candle[], book: Book): Signals {
  return {
    momentum: momentum(candles),
    volume: volumePressure(candles),
    volatility: volatilityRegime(candles),
    book: bookImbalance(book),
  };
}

/** Volatility is context for Risk, not a direction, so it is excluded from the composite. */
function composite(s: Signals): number {
  return clamp(WEIGHTS.momentum * s.momentum + WEIGHTS.volume * s.volume + WEIGHTS.book * s.book);
}

// ---------- feed ----------

interface SymbolState {
  candles: Candle[];
  book: Book;
  price: number;
  lastTradeAt: number;
}

const WS_BASE = process.env.BINANCE_WS_URL ?? 'wss://stream.binance.com:9443';
const REST_BASE = process.env.BINANCE_REST_URL ?? 'https://api.binance.com';
const MAX_CANDLES = 200;

class MarketFeed extends EventEmitter {
  readonly state = new Map<string, SymbolState>();
  connected = false;
  private ws?: WebSocket;
  private retry = 0;
  private lastMsg = 0;
  private watchdog?: NodeJS.Timeout;

  constructor(private symbols: string[]) {
    super();
    for (const s of symbols) {
      this.state.set(s, { candles: [], book: { bids: [], asks: [] }, price: 0, lastTradeAt: 0 });
    }
  }

  async start() {
    await Promise.all(this.symbols.map((s) => this.backfill(s)));
    this.connect();
    this.watchdog = setInterval(() => {
      if (this.connected && Date.now() - this.lastMsg > 10_000) {
        console.warn('feed silent for 10s, reconnecting');
        this.ws?.terminate();
      }
    }, 5_000);
  }

  stop() {
    clearInterval(this.watchdog);
    this.ws?.removeAllListeners();
    this.ws?.close();
  }

  private async backfill(symbol: string) {
    try {
      const r = await fetch(`${REST_BASE}/api/v3/klines?symbol=${symbol}&interval=1m&limit=120`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const rows = (await r.json()) as (string | number)[][];
      const st = this.state.get(symbol)!;
      st.candles = rows.map((k) => ({
        t: Number(k[0]), o: Number(k[1]), h: Number(k[2]), l: Number(k[3]), c: Number(k[4]), v: Number(k[5]),
      }));
      st.price = st.candles.at(-1)?.c ?? 0;
      console.log(`backfilled ${symbol}: ${st.candles.length} candles`);
    } catch (e) {
      console.error(`backfill failed for ${symbol}:`, (e as Error).message);
    }
  }

  private connect() {
    const streams = this.symbols
      .flatMap((s) => {
        const l = s.toLowerCase();
        return [`${l}@trade`, `${l}@kline_1m`, `${l}@depth20@100ms`];
      })
      .join('/');
    const ws = new WebSocket(`${WS_BASE}/stream?streams=${streams}`);
    this.ws = ws;

    ws.on('open', () => {
      this.connected = true;
      this.retry = 0;
      this.lastMsg = Date.now();
      console.log('feed connected');
      this.emit('status', true);
    });
    ws.on('message', (raw) => {
      this.lastMsg = Date.now();
      try {
        this.handle(JSON.parse(raw.toString()));
      } catch {
        /* ignore malformed frame */
      }
    });
    ws.on('error', (e) => console.error('feed error:', e.message));
    ws.on('close', () => {
      this.connected = false;
      this.emit('status', false);
      const wait = Math.min(15_000, 500 * 2 ** this.retry++);
      console.warn(`feed closed, retrying in ${wait}ms`);
      setTimeout(() => this.connect(), wait);
    });
  }

  private handle(msg: { stream?: string; data?: any }) {
    if (!msg.stream || !msg.data) return;
    const [pair, kind] = msg.stream.split('@') as [string, string];
    const st = this.state.get(pair.toUpperCase());
    if (!st) return;
    const d = msg.data;

    if (kind === 'trade') {
      st.price = Number(d.p);
      st.lastTradeAt = Date.now();
    } else if (kind === 'kline_1m') {
      const k = d.k;
      const c: Candle = { t: k.t, o: +k.o, h: +k.h, l: +k.l, c: +k.c, v: +k.v };
      const last = st.candles[st.candles.length - 1];
      if (last && last.t === c.t) st.candles[st.candles.length - 1] = c;
      else st.candles.push(c);
      if (st.candles.length > MAX_CANDLES) st.candles.shift();
    } else if (kind.startsWith('depth')) {
      st.book = {
        bids: (d.bids as string[][]).map(([p, q]) => [+p!, +q!]),
        asks: (d.asks as string[][]).map(([p, q]) => [+p!, +q!]),
      };
    }
  }
}

// ---------- server ----------

const PORT = Number(process.env.PORT ?? 3000);
const TOKEN = process.env.AUTH_TOKEN ?? '';
const SYMBOLS = (process.env.SYMBOLS ?? 'BTCUSDT,ETHUSDT,SOLUSDT').split(',').map((s) => s.trim().toUpperCase());

const feed = new MarketFeed(SYMBOLS);
const indexPath = fileURLToPath(new URL('./index.html', import.meta.url));

const authed = (url: URL) => !TOKEN || url.searchParams.get('token') === TOKEN;

const http = createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', 'http://localhost');
  if (url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return void res.end(JSON.stringify({ ok: true, feed: feed.connected }));
  }
  if (!authed(url)) {
    res.writeHead(401);
    return void res.end('Unauthorized');
  }
  try {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(await readFile(indexPath));
  } catch {
    res.writeHead(500);
    res.end('index.html missing');
  }
});

const wss = new WebSocketServer({ noServer: true });
http.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url ?? '/', 'http://localhost');
  if (url.pathname !== '/ws' || !authed(url)) {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    return void socket.destroy();
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws));
});

function snapshot(): Snapshot {
  const now = Date.now();
  const symbols: SymbolSnapshot[] = SYMBOLS.map((symbol) => {
    const st = feed.state.get(symbol)!;
    const signals = computeSignals(st.candles, st.book);
    return {
      symbol,
      price: st.price,
      signals,
      composite: composite(signals),
      candles: st.candles.length,
      stale: !feed.connected || now - st.lastTradeAt > 15_000,
    };
  });
  return { type: 'snapshot', ts: now, connected: feed.connected, symbols };
}

setInterval(() => {
  if (wss.clients.size === 0) return;
  const msg = JSON.stringify(snapshot());
  for (const c of wss.clients) if (c.readyState === WebSocket.OPEN) c.send(msg);
}, 500);

await feed.start();
http.listen(PORT, () => console.log(`desk on http://localhost:${PORT}${TOKEN ? '/?token=...' : ''}`));
process.on('SIGINT', () => { feed.stop(); process.exit(0); });
