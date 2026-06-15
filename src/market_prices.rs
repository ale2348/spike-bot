//! Live best-ask prices for current 5m Up/Down tokens via Polymarket WebSocket.

use crate::discovery::{build_5m_slug, current_5m_period_start, MarketDiscovery};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_MARKET_PATH: &str = "/ws/market";

/// Best ask per token id (CLOB asset id).
#[derive(Debug, Default)]
pub struct AskPriceStore {
    asks: HashMap<String, f64>,
}

impl AskPriceStore {
    pub fn get_ask(&self, token_id: &str) -> Option<f64> {
        self.asks.get(token_id).copied()
    }

    fn set_ask(&mut self, token_id: &str, ask: f64) {
        self.asks.insert(token_id.to_string(), ask);
    }
}

#[derive(Debug, Clone)]
pub struct SymbolMarketTokens {
    pub symbol: String,
    pub slug: String,
    pub condition_id: String,
    pub up_token: String,
    pub down_token: String,
}

/// Maps symbol → current 5m market token ids.
#[derive(Debug, Default)]
pub struct MarketTokenRegistry {
    pub period_start: i64,
    pub markets: HashMap<String, SymbolMarketTokens>,
}

impl MarketTokenRegistry {
    pub fn all_token_ids(&self) -> Vec<String> {
        let mut ids = Vec::with_capacity(self.markets.len() * 2);
        for m in self.markets.values() {
            ids.push(m.up_token.clone());
            ids.push(m.down_token.clone());
        }
        ids
    }

    pub fn get(&self, symbol: &str) -> Option<&SymbolMarketTokens> {
        self.markets.get(&symbol.to_lowercase())
    }
}

/// Refresh current-period 5m markets and push token ids to the WS subscriber.
pub async fn run_market_token_refresh(
    discovery: MarketDiscovery,
    api: Arc<crate::api::PolymarketApi>,
    symbols: Vec<String>,
    store: Arc<RwLock<AskPriceStore>>,
    registry: Arc<RwLock<MarketTokenRegistry>>,
    subscribe_tx: mpsc::Sender<Vec<String>>,
) {
    let mut last_period = 0i64;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    loop {
        interval.tick().await;
        let period = current_5m_period_start();
        let period_changed = period != last_period;

        if period_changed {
            match refresh_registry(&discovery, &symbols, period).await {
                Ok(new_registry) => {
                    let token_ids = new_registry.all_token_ids();
                    if token_ids.is_empty() {
                        warn!("No active 5m markets found for symbols {:?}", symbols);
                    } else {
                        seed_asks_from_rest(&api, &store, &token_ids).await;
                        *registry.write().await = new_registry;
                        info!(
                            "5m market tokens refreshed (period {}) — {} token(s) across {:?}",
                            period,
                            token_ids.len(),
                            symbols
                        );
                        last_period = period;
                        if subscribe_tx.send(token_ids).await.is_err() {
                            warn!("Market price WS subscriber dropped");
                            return;
                        }
                    }
                }
                Err(e) => warn!("Failed to refresh 5m market tokens: {}", e),
            }
        }
    }
}

/// Refresh registry + ask prices immediately when a signal arrives before the background poll.
pub async fn ensure_registry_for_period(
    discovery: &MarketDiscovery,
    api: &crate::api::PolymarketApi,
    symbols: &[String],
    period_start: i64,
    store: &Arc<RwLock<AskPriceStore>>,
    registry: &Arc<RwLock<MarketTokenRegistry>>,
) -> Result<()> {
    {
        let reg = registry.read().await;
        if reg.period_start == period_start && !reg.markets.is_empty() {
            return Ok(());
        }
    }

    info!(
        "On-demand market registry refresh for period {} (background refresh lagging)",
        period_start
    );
    let new_registry = refresh_registry(discovery, symbols, period_start).await?;
    if new_registry.markets.is_empty() {
        anyhow::bail!("No active 5m markets for period {}", period_start);
    }
    let token_ids = new_registry.all_token_ids();
    seed_asks_from_rest(api, store, &token_ids).await;
    *registry.write().await = new_registry;
    Ok(())
}

async fn seed_asks_from_rest(
    api: &crate::api::PolymarketApi,
    store: &Arc<RwLock<AskPriceStore>>,
    token_ids: &[String],
) {
    for token_id in token_ids {
        match api.get_best_price(token_id).await {
            Ok(Some(tp)) => {
                if let Some(ask) = tp.ask {
                    let ask_f: f64 = ask.to_string().parse().unwrap_or(0.0);
                    if ask_f > 0.0 {
                        store.write().await.set_ask(token_id, ask_f);
                    }
                }
            }
            Ok(None) => {}
            Err(e) => debug!("REST ask seed failed for {}: {}", token_id, e),
        }
    }
}

async fn refresh_registry(
    discovery: &MarketDiscovery,
    symbols: &[String],
    period_start: i64,
) -> Result<MarketTokenRegistry> {
    let mut markets = HashMap::new();
    for symbol in symbols {
        let slug = build_5m_slug(symbol, period_start);
        let Some(condition_id) = discovery.get_5m_market(symbol, period_start).await? else {
            debug!("Market not active yet: {}", slug);
            continue;
        };
        let (up_token, down_token) = discovery.get_market_tokens(&condition_id).await?;
        markets.insert(
            symbol.clone(),
            SymbolMarketTokens {
                symbol: symbol.clone(),
                slug,
                condition_id,
                up_token,
                down_token,
            },
        );
    }
    Ok(MarketTokenRegistry {
        period_start,
        markets,
    })
}

/// Polymarket market-channel WebSocket; reconnects on disconnect or token subscription change.
pub async fn run_polymarket_ws_with_refresh(
    ws_base_url: String,
    store: Arc<RwLock<AskPriceStore>>,
    mut subscribe_rx: mpsc::Receiver<Vec<String>>,
) {
    let mut connect_backoff_ms = 0u64;
    let mut current_ids: Vec<String> = Vec::new();

    loop {
        if current_ids.is_empty() {
            match subscribe_rx.recv().await {
                Some(ids) => current_ids = ids,
                None => return,
            }
        }

        let url = format!(
            "{}{}",
            ws_base_url.trim_end_matches('/'),
            WS_MARKET_PATH
        );

        match connect_and_stream(&url, &current_ids, &store, &mut subscribe_rx).await {
            Ok(StreamOutcome::Resubscribe(new_ids)) => {
                current_ids = new_ids;
                connect_backoff_ms = 0;
            }
            Ok(StreamOutcome::Disconnected) => {
                warn!("Polymarket WebSocket disconnected — reconnecting immediately");
                connect_backoff_ms = 0;
            }
            Ok(StreamOutcome::Shutdown) => return,
            Err(e) => {
                let wait_ms = connect_backoff_ms;
                if wait_ms == 0 {
                    error!("Polymarket WebSocket error: {} — reconnecting immediately", e);
                } else {
                    error!(
                        "Polymarket WebSocket error: {} — reconnecting in {}ms",
                        e, wait_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                }
                connect_backoff_ms = next_connect_backoff_ms(connect_backoff_ms);
            }
        }
    }
}

fn next_connect_backoff_ms(current: u64) -> u64 {
    match current {
        0 => 250,
        250 => 500,
        500 => 1_000,
        other => (other * 2).min(5_000),
    }
}

enum StreamOutcome {
    /// 5m period rolled — reconnect with new token ids.
    Resubscribe(Vec<String>),
    /// Connection dropped — reconnect with same token ids.
    Disconnected,
    /// Subscribe channel closed — stop the feed.
    Shutdown,
}

async fn connect_and_stream(
    url: &str,
    token_ids: &[String],
    store: &Arc<RwLock<AskPriceStore>>,
    subscribe_rx: &mut mpsc::Receiver<Vec<String>>,
) -> Result<StreamOutcome> {
    let (ws, _) = connect_async(url)
        .await
        .context("Polymarket WS connect failed")?;
    info!(
        "Polymarket market WebSocket connected — {} token(s)",
        token_ids.len()
    );

    let (mut write, mut read) = ws.split();
    send_subscribe(&mut write, token_ids).await?;

    let ping_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            if write.send(Message::Text("PING".into())).await.is_err() {
                break;
            }
        }
    });

    let subscribed_set: HashSet<String> = token_ids.iter().cloned().collect();

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) if text != "PONG" => {
                        let mut guard = store.write().await;
                        apply_ws_message(&mut guard, &text);
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let reason = frame
                            .map(|f| f.reason.to_string())
                            .unwrap_or_default();
                        warn!(
                            "Polymarket WebSocket closed by server{}",
                            if reason.is_empty() {
                                String::new()
                            } else {
                                format!(": {reason}")
                            }
                        );
                        break;
                    }
                    Some(Err(e)) => {
                        warn!("Polymarket WebSocket read error: {} — will reconnect", e);
                        break;
                    }
                    None => {
                        warn!("Polymarket WebSocket stream ended — will reconnect");
                        break;
                    }
                    _ => {}
                }
            }
            new_ids = subscribe_rx.recv() => {
                match new_ids {
                    Some(ids) => {
                        let new_set: HashSet<String> = ids.iter().cloned().collect();
                        if new_set != subscribed_set {
                            ping_task.abort();
                            return Ok(StreamOutcome::Resubscribe(ids));
                        }
                    }
                    None => {
                        ping_task.abort();
                        return Ok(StreamOutcome::Shutdown);
                    }
                }
            }
        }
    }

    ping_task.abort();
    Ok(StreamOutcome::Disconnected)
}

async fn send_subscribe(
    write: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        Message,
    >,
    token_ids: &[String],
) -> Result<()> {
    let payload = serde_json::json!({
        "assets_ids": token_ids,
        "type": "market",
        "custom_feature_enabled": true
    });
    write
        .send(Message::Text(payload.to_string()))
        .await
        .context("Failed to send Polymarket WS subscribe")?;
    Ok(())
}

fn apply_ws_message(store: &mut AskPriceStore, text: &str) {
    let Ok(json) = serde_json::from_str::<Value>(text) else {
        return;
    };

    let event_type = json
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match event_type {
        "best_bid_ask" => {
            if let (Some(asset_id), Some(ask_str)) = (
                json.get("asset_id").and_then(|v| v.as_str()),
                json.get("best_ask").and_then(|v| v.as_str()),
            ) {
                if let Ok(ask) = ask_str.parse::<f64>() {
                    store.set_ask(asset_id, ask);
                }
            }
        }
        "book" => {
            if let (Some(asset_id), Some(asks)) = (
                json.get("asset_id").and_then(|v| v.as_str()),
                json.get("asks").and_then(|v| v.as_array()),
            ) {
                if let Some(best) = best_ask_from_book_levels(asks) {
                    store.set_ask(asset_id, best);
                }
            }
        }
        "price_change" => {
            if let Some(changes) = json.get("price_changes").and_then(|v| v.as_array()) {
                for change in changes {
                    if let (Some(asset_id), Some(ask_str)) = (
                        change.get("asset_id").and_then(|v| v.as_str()),
                        change.get("best_ask").and_then(|v| v.as_str()),
                    ) {
                        if let Ok(ask) = ask_str.parse::<f64>() {
                            if ask > 0.0 {
                                store.set_ask(asset_id, ask);
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn best_ask_from_book_levels(asks: &[Value]) -> Option<f64> {
    asks.iter()
        .filter_map(|level| {
            let price_str = level.get("price")?.as_str()?;
            price_str.parse::<f64>().ok()
        })
        .filter(|p| *p > 0.0 && *p < 1.0)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}
