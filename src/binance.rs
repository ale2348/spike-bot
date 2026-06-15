//! Live crypto price feed from Binance WebSocket (bookTicker streams).

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const BINANCE_WS_BASE: &str = "wss://stream.binance.com:9443/stream";

/// Normalized price tick (mid of best bid/ask).
#[derive(Debug, Clone)]
pub struct PriceTick {
    pub symbol: String,
    pub price: f64,
    pub timestamp_ms: u64,
}

/// Map bot symbol (btc) → Binance pair (btcusdt).
fn to_binance_pair(symbol: &str) -> String {
    format!("{}usdt", symbol.to_lowercase())
}

/// Map Binance pair (BTCUSDT) → bot symbol (btc).
fn from_binance_pair(pair: &str) -> String {
    pair.to_lowercase()
        .strip_suffix("usdt")
        .unwrap_or(&pair.to_lowercase())
        .to_string()
}

fn build_stream_url(symbols: &[String]) -> Result<Url> {
    let streams: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@bookTicker", to_binance_pair(s)))
        .collect();
    let url = format!("{}?streams={}", BINANCE_WS_BASE, streams.join("/"));
    Url::parse(&url).context("Invalid Binance WebSocket URL")
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Deserialize)]
struct CombinedStreamMessage {
    #[serde(rename = "stream")]
    _stream: String,
    data: BookTickerEvent,
}

#[derive(Debug, Deserialize)]
struct BookTickerEvent {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    best_bid: String,
    #[serde(rename = "a")]
    best_ask: String,
}

/// Subscribe to live bookTicker streams and forward normalized mid-price ticks.
pub async fn run_price_feed(
    symbols: Vec<String>,
    tx: mpsc::Sender<PriceTick>,
) -> Result<()> {
    let url = build_stream_url(&symbols)?;
    info!(
        "Connecting to Binance bookTicker WebSocket for {:?} …",
        symbols
    );

    let (ws, _) = connect_async(url.as_str())
        .await
        .context("Failed to connect to Binance WebSocket")?;

    info!("Binance WebSocket connected");

    let (mut write, mut read) = ws.split();

    // Binance closes idle connections after ~24h; keepalive every 3 minutes.
    let ping_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(180));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if write.send(Message::Ping(vec![])).await.is_err() {
                break;
            }
        }
    });

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Some(tick) = parse_tick(&text) {
                    if tx.send(tick).await.is_err() {
                        warn!("Price tick receiver dropped, stopping Binance feed");
                        break;
                    }
                }
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Ok(Message::Close(frame)) => {
                let reason = frame
                    .map(|f| f.reason.to_string())
                    .unwrap_or_default();
                warn!(
                    "Binance WebSocket closed by server{}",
                    if reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {reason}")
                    }
                );
                break;
            }
            Err(e) => {
                error!("Binance WebSocket read error: {}", e);
                break;
            }
            _ => {}
        }
    }

    ping_task.abort();
    Ok(())
}

fn parse_tick(text: &str) -> Option<PriceTick> {
    let wrapper: CombinedStreamMessage = serde_json::from_str(text).ok()?;
    let bid: f64 = wrapper.data.best_bid.parse().ok()?;
    let ask: f64 = wrapper.data.best_ask.parse().ok()?;
    if bid <= 0.0 || ask <= 0.0 || ask < bid {
        return None;
    }
    let symbol = from_binance_pair(&wrapper.data.symbol);
    Some(PriceTick {
        symbol,
        price: (bid + ask) / 2.0,
        timestamp_ms: now_ms(),
    })
}

/// Run the feed with automatic reconnection.
/// Clean disconnects reconnect immediately; failed connects use short exponential backoff.
pub async fn run_price_feed_with_reconnect(
    symbols: Vec<String>,
    tx: mpsc::Sender<PriceTick>,
) {
    let mut connect_backoff_ms = 0u64;

    loop {
        match run_price_feed(symbols.clone(), tx.clone()).await {
            Ok(()) => {
                warn!("Binance WebSocket disconnected — reconnecting immediately");
                connect_backoff_ms = 0;
            }
            Err(e) => {
                let wait_ms = connect_backoff_ms;
                if wait_ms == 0 {
                    error!("Binance WebSocket error: {} — reconnecting immediately", e);
                } else {
                    error!(
                        "Binance WebSocket error: {} — reconnecting in {}ms",
                        e, wait_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                }
                connect_backoff_ms = next_connect_backoff_ms(connect_backoff_ms);
            }
        }
    }
}

/// Backoff only for repeated connection failures (not clean disconnects).
fn next_connect_backoff_ms(current: u64) -> u64 {
    match current {
        0 => 250,
        250 => 500,
        500 => 1_000,
        other => (other * 2).min(5_000),
    }
}

/// If no tick arrives for this long, clear history for that symbol (likely WS outage).
const PRICE_GAP_RESET_MS: u64 = 4_000;

/// In-memory price history for momentum calculation.
pub struct PriceHistory {
    entries: HashMap<String, Vec<(u64, f64)>>,
    max_age_ms: u64,
}

impl PriceHistory {
    pub fn new(max_age_secs: u64) -> Self {
        Self {
            entries: HashMap::new(),
            max_age_ms: max_age_secs * 1000,
        }
    }

    pub fn push(&mut self, tick: &PriceTick) {
        let buf = self.entries.entry(tick.symbol.clone()).or_default();
        if let Some((last_ts, _)) = buf.last().copied() {
            let gap_ms = tick.timestamp_ms.saturating_sub(last_ts);
            if gap_ms > PRICE_GAP_RESET_MS {
                warn!(
                    "Price gap for {} — {}ms since last tick, clearing momentum history",
                    tick.symbol, gap_ms
                );
                buf.clear();
            }
        }
        buf.push((tick.timestamp_ms, tick.price));
        let cutoff = tick.timestamp_ms.saturating_sub(self.max_age_ms);
        buf.retain(|(ts, _)| *ts >= cutoff);
    }

    /// Price change (current − price at or before `lookback_ms` ago). Returns None if insufficient history.
    pub fn price_change(&self, symbol: &str, lookback_ms: u64, now_ms: u64) -> Option<f64> {
        let buf = self.entries.get(symbol)?;
        if buf.is_empty() {
            return None;
        }
        let current = buf.last()?.1;
        let target_ts = now_ms.saturating_sub(lookback_ms);

        let mut ref_price = None;
        for (ts, price) in buf.iter() {
            if *ts <= target_ts {
                ref_price = Some(*price);
            } else {
                break;
            }
        }
        let ref_price = ref_price?;
        Some(current - ref_price)
    }
}
