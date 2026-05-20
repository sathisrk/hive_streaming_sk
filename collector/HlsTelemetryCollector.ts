/**
 * HlsTelemetryCollector.ts
 *
 * Collects buffering and quality telemetry from an HLS.js player instance
 * and emits structured 30-second heartbeat payloads to a backend endpoint.
 *
 * Scope (per client confirmation):
 *   - P2P traffic is out of scope — p2pTraffic fields are zeroed.
 *     When the P2P SDK is integrated, hook into its segment-received event
 *     and increment p2pTraffic.receivedData. No schema change needed.
 *   - clientId is stable across reconnects — callers should persist it in
 *     sessionStorage and pass it in on construction.
 *   - agent_ts marks the END of each 30s window (confirmed by client) — the
 *     collector fires the payload at the end of the interval, which is correct.
 *
 * Design decisions:
 *   - Accumulates per-window counts/durations in a plain object — no
 *     unbounded arrays. At 30s intervals this is negligible overhead.
 *   - sendBeacon() for the final flush on page unload. It survives tab close
 *     and is fire-and-forget. Falls back to fetch() for mid-session sends.
 *   - Exponential backoff on failed sends (1s, 2s, 4s). We give up after
 *     maxRetries — stale buffering data is worse than missing data because
 *     it skews session-level aggregates.
 *   - No external deps beyond hls.js.
 *
 * Usage:
 *   import Hls from 'hls.js';
 *   import { HlsTelemetryCollector } from './HlsTelemetryCollector';
 *
 *   const hls = new Hls();
 *   hls.loadSource(streamUrl);
 *   hls.attachMedia(videoElement);
 *
 *   const collector = new HlsTelemetryCollector(hls, {
 *     endpoint:    'https://telemetry.hivestreaming.com/ingest',
 *     customerId:  'acme-corp',
 *     contentId:   'webcast-2025-q4-kickoff',
 *     clientId:    sessionStorage.getItem('hive_client_id') ?? crypto.randomUUID(),
 *     intervalMs:  30_000,
 *   });
 *   collector.start();
 *
 *   // Teardown (e.g. component unmount):
 *   collector.stop();
 */

import Hls from 'hls.js';

// --------------------------------------------------------------------------
// Config
// --------------------------------------------------------------------------

export interface CollectorConfig {
  endpoint:    string;
  customerId:  string;
  contentId:   string;
  /** Stable session identifier — caller is responsible for persistence */
  clientId:    string;
  /** Heartbeat emission interval in ms. Should match backend expectation. Default: 30_000 */
  intervalMs?: number;
  /** Max send attempts per payload before dropping. Default: 3 */
  maxRetries?: number;
}

// --------------------------------------------------------------------------
// Payload types — mirror the Delta table schema exactly
// --------------------------------------------------------------------------

interface TrafficStats {
  requests:      number;
  responses:     number;
  requestedData: number;  // bytes
  receivedData:  number;  // bytes
}

interface DistributionStats {
  sourceTraffic: TrafficStats;
  p2pTraffic:    TrafficStats;  // always zero per current scope
}

export interface TelemetryPayload {
  customerId:          string;
  contentId:           string;
  clientId:            string;
  eventDate:           string;  // "YYYY-MM-DD"
  timestampInfo: {
    agent: number;              // Unix ms — end of the 30s window (confirmed)
  };
  player: {
    bufferings:    number;      // # buffering events this window
    bufferingTime: number;      // total ms buffering
  };
  totalDistribution:    DistributionStats;
  qualityDistribution:  Record<string, DistributionStats>;
}

// --------------------------------------------------------------------------
// Internal accumulator — reset after each heartbeat
// --------------------------------------------------------------------------

interface WindowState {
  bufferings:             number;
  bufferingTimeMs:        number;
  activeBufferStartMs:    number | null;   // non-null while currently buffering
  currentQuality:         string | null;
  // per quality label: { bytesRequested, bytesReceived, requestCount }
  qualityStats: Record<string, { requested: number; received: number; requests: number }>;
}

// --------------------------------------------------------------------------
// Collector
// --------------------------------------------------------------------------

export class HlsTelemetryCollector {
  private readonly hls:    Hls;
  private readonly config: Required<CollectorConfig>;
  private state:           WindowState;
  private intervalId:      ReturnType<typeof setInterval> | null = null;
  private stopped = false;

  constructor(hls: Hls, config: CollectorConfig) {
    this.hls    = hls;
    this.config = { intervalMs: 30_000, maxRetries: 3, ...config };
    this.state  = this.emptyWindow();
  }

  // --------------------------------------------------------------------------
  // Lifecycle
  // --------------------------------------------------------------------------

  start(): void {
    if (this.intervalId !== null) return;  // idempotent

    this.attachListeners();

    this.intervalId = setInterval(() => {
      this.flush();
    }, this.config.intervalMs);

    window.addEventListener('pagehide', this.onPageHide);
  }

  stop(): void {
    if (this.intervalId !== null) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    window.removeEventListener('pagehide', this.onPageHide);
    this.detachListeners();
    this.flush();         // drain whatever's in the current window
    this.stopped = true;
  }

  // --------------------------------------------------------------------------
  // HLS.js + media element event listeners
  // --------------------------------------------------------------------------

  private attachListeners(): void {
    // Use the media element's 'waiting' / 'playing' events as the buffering
    // signal — they fire regardless of whether the stall is HLS-level or
    // network-level, so we don't miss anything.
    const media = this.hls.media;
    if (media) {
      media.addEventListener('waiting', this.onBufferingStart);
      media.addEventListener('stalled', this.onBufferingStart);
      media.addEventListener('playing', this.onBufferingEnd);
    }

    // Quality switches — HLS.js fires this after ABR selects a new level
    this.hls.on(Hls.Events.LEVEL_SWITCHED, this.onLevelSwitched);

    // Segment load events — gives us bytes per quality label
    this.hls.on(Hls.Events.FRAG_LOADING, this.onFragLoading);
    this.hls.on(Hls.Events.FRAG_LOADED,  this.onFragLoaded);
  }

  private detachListeners(): void {
    const media = this.hls.media;
    if (media) {
      media.removeEventListener('waiting', this.onBufferingStart);
      media.removeEventListener('stalled', this.onBufferingStart);
      media.removeEventListener('playing', this.onBufferingEnd);
    }
    this.hls.off(Hls.Events.LEVEL_SWITCHED, this.onLevelSwitched);
    this.hls.off(Hls.Events.FRAG_LOADING,   this.onFragLoading);
    this.hls.off(Hls.Events.FRAG_LOADED,    this.onFragLoaded);
  }

  // --------------------------------------------------------------------------
  // Event handlers
  // --------------------------------------------------------------------------

  private onBufferingStart = (): void => {
    if (this.state.activeBufferStartMs !== null) return;  // already counting
    this.state.activeBufferStartMs = Date.now();
    this.state.bufferings += 1;
  };

  private onBufferingEnd = (): void => {
    if (this.state.activeBufferStartMs === null) return;
    this.state.bufferingTimeMs += Date.now() - this.state.activeBufferStartMs;
    this.state.activeBufferStartMs = null;
  };

  private onLevelSwitched = (_event: string, data: { level: number }): void => {
    const level = this.hls.levels[data.level];
    if (!level) return;
    // Derive label from resolution height — matches the backend schema (e.g. "1080p")
    this.state.currentQuality = level.height ? `${level.height}p` : `level_${data.level}`;
  };

  private onFragLoading = (_event: string, data: { frag: { stats?: { total?: number } } }): void => {
    const label = this.state.currentQuality ?? 'unknown';
    const s = this.qualityStatsFor(label);
    s.requests  += 1;
    s.requested += data.frag?.stats?.total ?? 0;
  };

  private onFragLoaded = (_event: string, data: { frag: { stats?: { loaded?: number } } }): void => {
    const label = this.state.currentQuality ?? 'unknown';
    this.qualityStatsFor(label).received += data.frag?.stats?.loaded ?? 0;
  };

  private onPageHide = (): void => {
    // sendBeacon is fire-and-forget and survives tab/window close.
    // It doesn't support callbacks so we can't retry — one shot.
    const blob = new Blob([JSON.stringify(this.buildPayload())], { type: 'application/json' });
    navigator.sendBeacon(this.config.endpoint, blob);
    this.state = this.emptyWindow();
  };

  // --------------------------------------------------------------------------
  // Window management
  // --------------------------------------------------------------------------

  private emptyWindow(): WindowState {
    return {
      bufferings:          0,
      bufferingTimeMs:     0,
      activeBufferStartMs: null,
      currentQuality:      null,
      qualityStats:        {},
    };
  }

  private qualityStatsFor(label: string) {
    if (!this.state.qualityStats[label]) {
      this.state.qualityStats[label] = { requested: 0, received: 0, requests: 0 };
    }
    return this.state.qualityStats[label];
  }

  private buildPayload(): TelemetryPayload {
    const s   = this.state;
    const now = Date.now();

    // If we're mid-buffer when the window closes, count time up to now.
    // The next window will start fresh — we don't carry partial buffering over.
    const bufferingTime = s.activeBufferStartMs !== null
      ? s.bufferingTimeMs + (now - s.activeBufferStartMs)
      : s.bufferingTimeMs;

    const zero = (): TrafficStats => ({ requests: 0, responses: 0, requestedData: 0, receivedData: 0 });

    // Build quality distribution — source traffic only (P2P zeroed per scope)
    const qualityDistribution: Record<string, DistributionStats> = {};
    for (const [label, stat] of Object.entries(s.qualityStats)) {
      qualityDistribution[label] = {
        sourceTraffic: {
          requests:      stat.requests,
          responses:     stat.requests,    // 1:1 assumption (no explicit response tracking)
          requestedData: stat.requested,
          receivedData:  stat.received,
        },
        p2pTraffic: zero(),   // P2P out of scope — hook here when SDK is available
      };
    }

    // Aggregate totals across quality levels
    const totalSource = Object.values(qualityDistribution).reduce(
      (acc, d) => ({
        requests:      acc.requests      + d.sourceTraffic.requests,
        responses:     acc.responses     + d.sourceTraffic.responses,
        requestedData: acc.requestedData + d.sourceTraffic.requestedData,
        receivedData:  acc.receivedData  + d.sourceTraffic.receivedData,
      }),
      zero(),
    );

    return {
      customerId:   this.config.customerId,
      contentId:    this.config.contentId,
      clientId:     this.config.clientId,
      eventDate:    new Date(now).toISOString().slice(0, 10),
      timestampInfo: { agent: now },   // end of window — confirmed correct by client
      player: {
        bufferings:    s.bufferings,
        bufferingTime: Math.round(bufferingTime),
      },
      totalDistribution: { sourceTraffic: totalSource, p2pTraffic: zero() },
      qualityDistribution,
    };
  }

  private flush(): void {
    if (this.stopped) return;
    const payload  = this.buildPayload();
    this.state     = this.emptyWindow();
    this.send(payload, 0);
  }

  // --------------------------------------------------------------------------
  // Transport — fetch with exponential backoff
  // --------------------------------------------------------------------------

  private async send(payload: TelemetryPayload, attempt: number): Promise<void> {
    try {
      const res = await fetch(this.config.endpoint, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        // keepalive: true lets the request outlive the page — safety net for
        // environments where sendBeacon isn't available (e.g. some WebViews)
        keepalive: true,
        body:    JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    } catch (err) {
      if (attempt < this.config.maxRetries) {
        // 1s → 2s → 4s backoff. Don't grow past 4s — the next window fires in 30s
        // and we don't want retry storms stacking up.
        setTimeout(() => this.send(payload, attempt + 1), Math.pow(2, attempt) * 1_000);
      } else {
        // Drop it. Stale buffering data would skew session aggregates more than
        // a missing data point would. Log for debugging but don't re-queue.
        console.warn('[HlsTelemetryCollector] Payload dropped after max retries:', err);
      }
    }
  }
}
