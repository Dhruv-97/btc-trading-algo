import { useEffect, useRef } from "react";
import * as PIXI from "pixi.js";

type Lvl = [number, number];

type OBMsg = {
  ts: number;
  symbol: string;
  bucket: number;
  ladder_levels: number;
  depth_levels: number;
  best_bid: number | null;
  best_ask: number | null;
  mid: number | null;
  spread: number | null;
  bids: Lvl[];       // bucketed ladder
  asks: Lvl[];
  depth_bids: Lvl[]; // raw depth curve
  depth_asks: Lvl[];
};

function fmtPrice(x: number) {
  return x.toLocaleString(undefined, { maximumFractionDigits: 0 });
}
function fmtQty(x: number) {
  if (x >= 1000) return (x / 1000).toFixed(1) + "k";
  if (x >= 100) return x.toFixed(1);
  return x.toFixed(2);
}

function makeVerticalGradientTexture(width: number, height: number, topRGBA: string, bottomRGBA: string) {
  const c = document.createElement("canvas");
  c.width = width;
  c.height = height;
  const g = c.getContext("2d")!;
  const grad = g.createLinearGradient(0, 0, 0, height);
  grad.addColorStop(0, topRGBA);
  grad.addColorStop(1, bottomRGBA);
  g.fillStyle = grad;
  g.fillRect(0, 0, width, height);
  return PIXI.Texture.from(c);
}

const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v));
const clamp01 = (x: number) => Math.max(0, Math.min(1, x));
const logNorm = (qty: number, maxQty: number) =>
  Math.log1p(Math.max(0, qty)) / Math.log1p(Math.max(1e-9, maxQty));

export default function App() {
  const hostRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    let cancelled = false;
    let ws: WebSocket | null = null;
    let app: any = null;

    // Layout
    const W = 1080;
    const H = 650;
    const pad = 18;

    const depthTop = 18;
    const depthH = 170;

    const ladderTop = depthTop + depthH + 28;
    const ladderH = H - ladderTop - 16;

    // Theme
    const theme = {
      bg: 0xffffff,
      text: 0x111827,
      subtle: 0x6b7280,
      divider: 0xe5e7eb,
      bid: 0x10b981,
      ask: 0xef4444,
      bidGradTop: "rgba(16,185,129,0.28)",
      bidGradBot: "rgba(16,185,129,0.04)",
      askGradTop: "rgba(239,68,68,0.28)",
      askGradBot: "rgba(239,68,68,0.04)",
    };

    const baseStyle = new PIXI.TextStyle({
      fontFamily: "system-ui, -apple-system, Segoe UI, Roboto",
      fontSize: 18,
      fill: theme.text,
    });
    const subtleStyle = new PIXI.TextStyle({
      fontFamily: "system-ui, -apple-system, Segoe UI, Roboto",
      fontSize: 14,
      fill: theme.subtle,
    });

    // Header texts
    let titleText: PIXI.Text;
    let subText: PIXI.Text;

    // Depth objects
    let bidMask: PIXI.Graphics;
    let askMask: PIXI.Graphics;
    let bidFillSprite: PIXI.Sprite;
    let askFillSprite: PIXI.Sprite;
    let bidLine: PIXI.Graphics;
    let askLine: PIXI.Graphics;
    let depthAxis: PIXI.Container;

    // Scrollable ladder objects
    let ladderViewport: PIXI.Container;
    let ladderContent: PIXI.Container;
    let ladderMask: PIXI.Graphics;
    let ladderBG: PIXI.Graphics;

    // Ladder drawing
    let ladderRects: PIXI.Graphics;
    let ladderText: PIXI.Container;

    // Text pooling
    const textPool: PIXI.Text[] = [];
    const borrowText = () => {
      const t = textPool.pop() ?? new PIXI.Text({ text: "", style: baseStyle });
      t.visible = true;
      ladderText.addChild(t);
      return t;
    };
    const reclaimTexts = () => {
      const kids = ladderText.children.slice();
      ladderText.removeChildren();
      for (const k of kids) {
        const t = k as PIXI.Text;
        t.visible = false;
        textPool.push(t);
      }
    };

    // Scroll state
    let scrollY = 0;
    let maxScroll = 0;
    const SCROLL_STEP = 60;

    // Log-scale max smoothing (per side)
    let smoothBidMax = 1;
    let smoothAskMax = 1;
    const MAX_SMOOTH = 0.15;

    // Data
    let latest: OBMsg | null = null;

    const init = async () => {
      if (!hostRef.current) return;

      app = new PIXI.Application();
      if (typeof app.init === "function") {
        await app.init({
          width: W,
          height: H,
          background: theme.bg,
          antialias: true,
          autoDensity: true,
          resolution: window.devicePixelRatio || 1,
        });
      } else {
        app = new PIXI.Application({
          width: W,
          height: H,
          backgroundColor: theme.bg,
          antialias: true,
          autoDensity: true,
          resolution: window.devicePixelRatio || 1,
        });
      }

      if (cancelled) return;

      const canvas: HTMLCanvasElement = app.canvas ?? app.view;
      hostRef.current.innerHTML = "";
      hostRef.current.appendChild(canvas);

      // Card border
      const card = new PIXI.Graphics()
        .roundRect(pad - 8, 10, W - 2 * (pad - 8), H - 20, 16)
        .stroke({ color: theme.divider, width: 1, alpha: 1 });
      app.stage.addChild(card);

      titleText = new PIXI.Text({
        text: "Order Book - Level 2",
        style: new PIXI.TextStyle({ ...subtleStyle, fontSize: 18 }),
      });
      titleText.position.set(pad, 14);
      app.stage.addChild(titleText);

      subText = new PIXI.Text({ text: "Connecting…", style: subtleStyle });
      subText.position.set(pad, 38);
      app.stage.addChild(subText);

      const sep = new PIXI.Graphics()
        .rect(pad - 8, ladderTop - 12, W - 2 * (pad - 8), 1)
        .fill({ color: theme.divider, alpha: 1 });
      app.stage.addChild(sep);

      // ===== Depth init
      const depthX0 = pad;
      const depthX1 = W - pad;

      const bidTex = makeVerticalGradientTexture(4, depthH, theme.bidGradTop, theme.bidGradBot);
      const askTex = makeVerticalGradientTexture(4, depthH, theme.askGradTop, theme.askGradBot);

      bidFillSprite = new PIXI.Sprite(bidTex);
      bidFillSprite.position.set(depthX0, depthTop);
      bidFillSprite.width = depthX1 - depthX0;
      bidFillSprite.height = depthH;

      askFillSprite = new PIXI.Sprite(askTex);
      askFillSprite.position.set(depthX0, depthTop);
      askFillSprite.width = depthX1 - depthX0;
      askFillSprite.height = depthH;

      bidMask = new PIXI.Graphics();
      askMask = new PIXI.Graphics();
      bidFillSprite.mask = bidMask;
      askFillSprite.mask = askMask;

      bidLine = new PIXI.Graphics();
      askLine = new PIXI.Graphics();
      depthAxis = new PIXI.Container();

      app.stage.addChild(bidFillSprite, askFillSprite, bidMask, askMask, bidLine, askLine, depthAxis);

      // ===== Ladder viewport (scrollable)
      ladderViewport = new PIXI.Container();
      ladderViewport.position.set(0, ladderTop);
      app.stage.addChild(ladderViewport);

      ladderContent = new PIXI.Container();
      ladderViewport.addChild(ladderContent);

      ladderMask = new PIXI.Graphics()
        .rect(pad - 8, 0, W - 2 * (pad - 8), ladderH)
        .fill({ color: 0xffffff, alpha: 1 });
      ladderViewport.addChild(ladderMask);
      ladderContent.mask = ladderMask;

      // event-capture background (invisible)
      ladderBG = new PIXI.Graphics()
        .rect(pad - 8, 0, W - 2 * (pad - 8), ladderH)
        .fill({ color: 0xffffff, alpha: 0.0001 });
      ladderBG.eventMode = "static";
      ladderBG.cursor = "grab";
      ladderViewport.addChild(ladderBG);

      ladderRects = new PIXI.Graphics();
      ladderText = new PIXI.Container();
      ladderContent.addChild(ladderRects, ladderText);

      // Wheel scroll
      const onWheel = (e: WheelEvent) => {
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;

        // only if inside ladder rect
        if (x < pad - 8 || x > W - (pad - 8)) return;
        if (y < ladderTop || y > ladderTop + ladderH) return;

        e.preventDefault();
        const dy = (e.deltaY > 0 ? 1 : -1) * SCROLL_STEP;
        scrollY = clamp(scrollY + dy, 0, maxScroll);
        ladderContent.y = -scrollY;
      };
      canvas.addEventListener("wheel", onWheel, { passive: false });

      // Drag scroll
      let dragging = false;
      let lastY = 0;

      ladderBG.on("pointerdown", (ev: any) => {
        dragging = true;
        ladderBG.cursor = "grabbing";
        lastY = ev.global.y;
      });
      const endDrag = () => {
        dragging = false;
        ladderBG.cursor = "grab";
      };
      ladderBG.on("pointerup", endDrag);
      ladderBG.on("pointerupoutside", endDrag);
      ladderBG.on("pointermove", (ev: any) => {
        if (!dragging) return;
        const dy = lastY - ev.global.y; // drag up => scroll down
        lastY = ev.global.y;
        scrollY = clamp(scrollY + dy, 0, maxScroll);
        ladderContent.y = -scrollY;
      });

      // Cleanup
      (app as any).__cleanup = () => {
        canvas.removeEventListener("wheel", onWheel as any);
      };

      // Ticker
      app.ticker.add(() => {
        if (!latest) return;
        draw(latest);
      });
    };

    function draw(m: OBMsg) {
      // ===== Header
      const sym = m.symbol ?? "BTCUSDT";
      const midStr = m.mid != null ? fmtPrice(m.mid) : "--";
      titleText.text = "Order Book - Level 2";
      subText.text = `${sym}   ${midStr}`;

      // ===== Clear depth + axis
      bidMask.clear();
      askMask.clear();
      bidLine.clear();
      askLine.clear();
      depthAxis.removeChildren();

      // ===== Clear ladder
      ladderRects.clear();
      reclaimTexts();

      // ===== Depth curve
      const depthX0 = pad;
      const depthX1 = W - pad;
      const depthY0 = depthTop;
      const depthY1 = depthTop + depthH;

      const bids = m.depth_bids ?? [];
      const asks = m.depth_asks ?? [];

      let c = 0;
      const bidCum: { p: number; c: number }[] = [];
      for (const [p, q] of bids) { c += q; bidCum.push({ p, c }); }

      c = 0;
      const askCum: { p: number; c: number }[] = [];
      for (const [p, q] of asks) { c += q; askCum.push({ p, c }); }

      const pMin = bids.length ? bids[bids.length - 1][0] : (m.mid ?? 0) - 1;
      const pMax = asks.length ? asks[asks.length - 1][0] : (m.mid ?? 0) + 1;

      const maxCum = Math.max(
        bidCum.length ? bidCum[bidCum.length - 1].c : 1,
        askCum.length ? askCum[askCum.length - 1].c : 1
      );

      const xScale = (p: number) => {
        if (pMax === pMin) return (depthX0 + depthX1) / 2;
        return depthX0 + ((p - pMin) / (pMax - pMin)) * (depthX1 - depthX0);
      };
      const yScale = (cum: number) => depthY1 - (cum / (maxCum || 1)) * (depthY1 - depthY0);

      if (bidCum.length >= 2) {
        bidMask.moveTo(xScale(bidCum[0].p), depthY1);
        for (const pt of bidCum) bidMask.lineTo(xScale(pt.p), yScale(pt.c));
        bidMask.lineTo(xScale(bidCum[bidCum.length - 1].p), depthY1);
        bidMask.closePath();
        bidMask.fill({ color: 0xffffff, alpha: 1 });

        bidLine.moveTo(xScale(bidCum[0].p), yScale(bidCum[0].c));
        for (const pt of bidCum) bidLine.lineTo(xScale(pt.p), yScale(pt.c));
        bidLine.stroke({ color: theme.bid, width: 4, alpha: 0.95 });
      }

      if (askCum.length >= 2) {
        askMask.moveTo(xScale(askCum[0].p), depthY1);
        for (const pt of askCum) askMask.lineTo(xScale(pt.p), yScale(pt.c));
        askMask.lineTo(xScale(askCum[askCum.length - 1].p), depthY1);
        askMask.closePath();
        askMask.fill({ color: 0xffffff, alpha: 1 });

        askLine.moveTo(xScale(askCum[0].p), yScale(askCum[0].c));
        for (const pt of askCum) askLine.lineTo(xScale(pt.p), yScale(pt.c));
        askLine.stroke({ color: theme.ask, width: 4, alpha: 0.95 });
      }

      const labels: Array<[string, number]> = [
        [fmtPrice(pMin), pMin],
        [m.mid != null ? fmtPrice(m.mid) : fmtPrice((pMin + pMax) / 2), m.mid ?? (pMin + pMax) / 2],
        [fmtPrice(pMax), pMax],
      ];
      for (const [txt, p] of labels) {
        const t = new PIXI.Text({ text: txt, style: subtleStyle });
        t.anchor.set(0.5, 0);
        t.position.set(xScale(p), depthY1 + 6);
        depthAxis.addChild(t);
      }

      // ===== Ladder (scrollable)
      const ladderBids = m.bids ?? [];
      const ladderAsks = m.asks ?? [];
      const rows = Math.max(ladderBids.length, ladderAsks.length);

      const tableW = W - 2 * pad;
      const colSizeW = 70;
      const colPriceW = (tableW - colSizeW * 2) / 2;

      const xSizeL = pad;
      const xBid = xSizeL + colSizeW;
      const xAsk = xBid + colPriceW;
      const xSizeR = xAsk + colPriceW;

      const headerY = 8;
      const startY = 36;
      const rowH = 36;

      // content height and scroll clamp
      const contentH = startY + rows * rowH + 10;
      maxScroll = Math.max(0, contentH - ladderH);
      scrollY = clamp(scrollY, 0, maxScroll);
      ladderContent.y = -scrollY;

      // headers (scroll with content; can make sticky if desired)
      const hSizeL = borrowText(); hSizeL.text = "Size"; hSizeL.style = subtleStyle; hSizeL.position.set(xSizeL + 6, headerY);
      const hBid  = borrowText();  hBid.text  = "Bid";  hBid.style  = subtleStyle; hBid.position.set(xBid + colPriceW / 2 - 14, headerY);
      const hAsk  = borrowText();  hAsk.text  = "Ask";  hAsk.style  = subtleStyle; hAsk.position.set(xAsk + colPriceW / 2 - 14, headerY);
      const hSizeR= borrowText();  hSizeR.text= "Size"; hSizeR.style= subtleStyle; hSizeR.position.set(xSizeR + 6, headerY);

      // divider through full content
      ladderRects
        .moveTo(xAsk, 0)
        .lineTo(xAsk, contentH)
        .stroke({ color: theme.divider, width: 1, alpha: 1 });

      // per-side max + smoothing for log-scale widths
      const bidSizes = ladderBids.map(x => x?.[1] ?? 0).filter(v => v > 0);
      const askSizes = ladderAsks.map(x => x?.[1] ?? 0).filter(v => v > 0);

      const bidMax = Math.max(...bidSizes, 1e-9);
      const askMax = Math.max(...askSizes, 1e-9);

      smoothBidMax = smoothBidMax + (bidMax - smoothBidMax) * MAX_SMOOTH;
      smoothAskMax = smoothAskMax + (askMax - smoothAskMax) * MAX_SMOOTH;

      for (let i = 0; i < rows; i++) {
        const y = startY + i * rowH;

        // row separator
        ladderRects
          .moveTo(pad, y + rowH)
          .lineTo(pad + tableW, y + rowH)
          .stroke({ color: theme.divider, width: 1, alpha: 0.7 });

        const bid = ladderBids[i] ?? null;
        const ask = ladderAsks[i] ?? null;

        // bars (log scale, per side)
        if (bid) {
          const frac = clamp01(logNorm(bid[1], smoothBidMax));
          const w = frac * (colPriceW - 10);
          ladderRects
            .roundRect(xBid + (colPriceW - 6) - w, y + 6, w, rowH - 12, 12)
            .fill({ color: theme.bid, alpha: 0.12 });
        }
        if (ask) {
          const frac = clamp01(logNorm(ask[1], smoothAskMax));
          const w = frac * (colPriceW - 10);
          ladderRects
            .roundRect(xAsk + 6, y + 6, w, rowH - 12, 12)
            .fill({ color: theme.ask, alpha: 0.12 });
        }

        // text
        const tSizeL = borrowText();
        tSizeL.text = bid ? fmtQty(bid[1]) : "";
        tSizeL.style = baseStyle;
        tSizeL.anchor.set(0, 0.5);
        tSizeL.position.set(xSizeL + 6, y + rowH / 2);

        const tBid = borrowText();
        tBid.text = bid ? fmtPrice(bid[0]) : "";
        tBid.style = new PIXI.TextStyle({ ...baseStyle, fontSize: 20, fill: theme.bid });
        tBid.anchor.set(0.5, 0.5);
        tBid.position.set(xBid + colPriceW / 2, y + rowH / 2);

        const tAsk = borrowText();
        tAsk.text = ask ? fmtPrice(ask[0]) : "";
        tAsk.style = new PIXI.TextStyle({ ...baseStyle, fontSize: 20, fill: theme.ask });
        tAsk.anchor.set(0.5, 0.5);
        tAsk.position.set(xAsk + colPriceW / 2, y + rowH / 2);

        const tSizeR = borrowText();
        tSizeR.text = ask ? fmtQty(ask[1]) : "";
        tSizeR.style = baseStyle;
        tSizeR.anchor.set(0, 0.5);
        tSizeR.position.set(xSizeR + 6, y + rowH / 2);
      }
    }

    const run = async () => {
      await init();
      if (cancelled) return;

      ws = new WebSocket("ws://localhost:8000/ws/orderbook");
      ws.onmessage = (ev) => {
        try {
          latest = JSON.parse(ev.data) as OBMsg;
        } catch {}
      };
    };

    run();

    return () => {
      cancelled = true;
      try { ws?.close(); } catch {}
      try { (app as any)?.__cleanup?.(); } catch {}
      try { app?.destroy(true); } catch {}
    };
  }, []);

  return (
    <div style={{ padding: 16, display: "flex", justifyContent: "center" }}>
      <div ref={hostRef} />
    </div>
  );
}
