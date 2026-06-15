//! Momentum strategy for 5m Up/Down markets:
//! 1. Subscribe live crypto mid-prices from Binance bookTicker (best bid/ask)
//! 2. Subscribe live Polymarket Up/Down best ask prices for current 5m markets
//! 3. Detect upward/downward momentum (price change over configurable lookback)
//! 4. Place limit buy on Up (rise) or Down (fall) token at current Polymarket ask
//! 5. Infer fill from best ask vs limit price; track resolution and PnL

use crate::trade_cache::TradedMarketsCache;
use crate::api::PolymarketApi;
use crate::binance::{self, PriceHistory, PriceTick};
use crate::config::Config;
use crate::discovery::{build_5m_slug, current_5m_period_start, MarketDiscovery};
use crate::market_prices::{
    ensure_registry_for_period, run_market_token_refresh, run_polymarket_ws_with_refresh,
    AskPriceStore, MarketTokenRegistry,
};
use crate::models::{OrderRequest, TradeSide};
use crate::performance::{manage_live_order, watch_order_fill, PerformanceTracker, run_resolution_poller};
use anyhow::Result;
use log::{info, warn};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock};

pub struct MomentumStrategy {
    api: Arc<PolymarketApi>,
    config: Config,
    discovery: MarketDiscovery,
}

impl Clone for MomentumStrategy {
    fn clone(&self) -> Self {
        Self {
            api: self.api.clone(),
            config: self.config.clone(),
            discovery: self.discovery.clone(),
        }
    }
}

impl MomentumStrategy {
    pub fn new(api: Arc<PolymarketApi>, config: Config) -> Self {
        let discovery = MarketDiscovery::new(api.clone());
        Self {
            api,
            config,
            discovery,
        }
    }

    pub async fn run(self) -> Result<()> {
        let strategy = Arc::new(self);
        let symbols = strategy.config.strategy.symbols.clone();
        let max_lookback = strategy
            .config
            .strategy
            .momentum
            .values()
            .map(|m| m.lookback_secs)
            .max()
            .unwrap_or(60)
            + 30;

        let ask_store = Arc::new(RwLock::new(AskPriceStore::default()));
        let market_registry = Arc::new(RwLock::new(MarketTokenRegistry::default()));
        let performance_log = strategy
            .config
            .strategy
            .performance_log
            .as_ref()
            .map(PathBuf::from);
        let tracker = Arc::new(Mutex::new(PerformanceTracker::new(performance_log)));

        let (market_sub_tx, market_sub_rx) = mpsc::channel::<Vec<String>>(8);

        let discovery = strategy.discovery.clone();
        let api = strategy.api.clone();
        let refresh_symbols = symbols.clone();
        let store_for_refresh = ask_store.clone();
        let registry_for_refresh = market_registry.clone();
        tokio::spawn(async move {
            run_market_token_refresh(
                discovery,
                api,
                refresh_symbols,
                store_for_refresh,
                registry_for_refresh,
                market_sub_tx,
            )
            .await;
        });

        let ws_url = strategy.config.polymarket.ws_url.clone();
        let store_clone = ask_store.clone();
        tokio::spawn(async move {
            run_polymarket_ws_with_refresh(ws_url, store_clone, market_sub_rx).await;
        });

        let api = strategy.api.clone();
        let tracker_clone = tracker.clone();
        let resolution_secs = strategy.config.strategy.resolution_check_secs;
        tokio::spawn(async move {
            run_resolution_poller(api, tracker_clone, resolution_secs).await;
        });

        let (tx, mut rx) = mpsc::channel::<PriceTick>(1024);
        let feed_symbols = symbols.clone();
        tokio::spawn(async move {
            binance::run_price_feed_with_reconnect(feed_symbols, tx).await;
        });

        let mut history = PriceHistory::new(max_lookback);
        let mut last_signal: HashMap<String, Instant> = HashMap::new();
        let traded_markets = Arc::new(Mutex::new(TradedMarketsCache::new(".")));

        info!(
            "Momentum strategy started — symbols: {:?}, limit: current ask, shares: {:.0}, cancel: {}s",
            symbols,
            strategy.config.strategy.trade_shares,
            strategy.config.strategy.cancel_after_secs,
        );
        info!(
            "Performance tracking enabled — resolution poll: {}s, log: {:?}",
            resolution_secs,
            strategy.config.strategy.performance_log
        );

        for sym in &symbols {
            if let Some(m) = strategy.config.momentum_for(sym) {
                info!(
                    "  {} — Up: +${:.4} / Down: -${:.4} in {}s",
                    sym, m.price_change_usd, m.price_change_usd, m.lookback_secs
                );
            } else {
                warn!("  {} — no momentum config, will be ignored", sym);
            }
        }

        while let Some(tick) = rx.recv().await {
            history.push(&tick);
            strategy
                .check_signal(
                    &tick,
                    &mut history,
                    &mut last_signal,
                    traded_markets.clone(),
                    market_registry.clone(),
                    ask_store.clone(),
                    tracker.clone(),
                )
                .await;
        }

        Ok(())
    }

    async fn check_signal(
        self: &Arc<Self>,
        tick: &PriceTick,
        history: &mut PriceHistory,
        last_signal: &mut HashMap<String, Instant>,
        traded_markets: Arc<Mutex<TradedMarketsCache>>,
        market_registry: Arc<RwLock<MarketTokenRegistry>>,
        ask_store: Arc<RwLock<AskPriceStore>>,
        tracker: Arc<Mutex<PerformanceTracker>>,
    ) {
        let momentum_cfg = match self.config.momentum_for(&tick.symbol) {
            Some(m) => m,
            None => return,
        };

        let lookback_ms = momentum_cfg.lookback_secs * 1000;
        let change = match history.price_change(&tick.symbol, lookback_ms, tick.timestamp_ms) {
            Some(c) => c,
            None => return,
        };

        let threshold = momentum_cfg.price_change_usd;
        let side = if change >= threshold {
            TradeSide::Up
        } else if change <= -threshold {
            TradeSide::Down
        } else {
            return;
        };

        let period_start = current_5m_period_start();
        let slug = build_5m_slug(&tick.symbol, period_start);
        if traded_markets.lock().await.is_blocked(&slug) {
            return;
        }

        let cooldown = Duration::from_secs(self.config.strategy.signal_cooldown_secs);
        if let Some(last) = last_signal.get(&tick.symbol) {
            if last.elapsed() < cooldown {
                return;
            }
        }

        last_signal.insert(tick.symbol.clone(), Instant::now());
        tracker.lock().await.record_signal();

        info!(
            "🚀 SIGNAL {} {} — ${:+.4} in {}s (price ${:.4}, threshold ±${:.4})",
            tick.symbol.to_uppercase(),
            side.as_str(),
            change,
            momentum_cfg.lookback_secs,
            tick.price,
            threshold
        );

        let strategy = Arc::clone(self);
        let tick = tick.clone();
        tokio::spawn(async move {
            if let Err(e) = strategy
                .execute_trade(
                    &tick,
                    change,
                    side,
                    traded_markets,
                    market_registry,
                    ask_store,
                    tracker,
                )
                .await
            {
                warn!("Trade failed for {} {}: {}", tick.symbol, side.as_str(), e);
                if let Some(path) = trades_log_path(&strategy.config) {
                    spawn_trade_log_failed(&path, &tick, change, side, &e.to_string());
                }
            }
        });
    }

    async fn execute_trade(
        &self,
        tick: &PriceTick,
        price_change: f64,
        side: TradeSide,
        traded_markets: Arc<Mutex<TradedMarketsCache>>,
        market_registry: Arc<RwLock<MarketTokenRegistry>>,
        ask_store: Arc<RwLock<AskPriceStore>>,
        tracker: Arc<Mutex<PerformanceTracker>>,
    ) -> Result<()> {
        let symbol = &tick.symbol;
        let period_start = current_5m_period_start();
        let slug = build_5m_slug(symbol, period_start);

        {
            let cache = traded_markets.lock().await;
            if cache.is_blocked(&slug) {
                info!(
                    "⏭️ Skipping {} — order already placed this period ({})",
                    symbol.to_uppercase(),
                    slug
                );
                return Ok(());
            }
        }

        traded_markets.lock().await.mark_pending(&slug);

        let result = self
            .execute_trade_inner(
                tick,
                price_change,
                side,
                &slug,
                period_start,
                traded_markets.clone(),
                market_registry,
                ask_store,
                tracker,
            )
            .await;

        if result.is_err() {
            traded_markets.lock().await.release_pending(&slug);
        }
        result
    }

    async fn execute_trade_inner(
        &self,
        tick: &PriceTick,
        price_change: f64,
        side: TradeSide,
        slug: &str,
        period_start: i64,
        traded_markets: Arc<Mutex<TradedMarketsCache>>,
        market_registry: Arc<RwLock<MarketTokenRegistry>>,
        ask_store: Arc<RwLock<AskPriceStore>>,
        tracker: Arc<Mutex<PerformanceTracker>>,
    ) -> Result<()> {
        let symbol = &tick.symbol;

        ensure_registry_for_period(
            &self.discovery,
            &self.api,
            &self.config.strategy.symbols,
            period_start,
            &ask_store,
            &market_registry,
        )
        .await?;

        let market = {
            let registry = market_registry.read().await;
            registry.get(symbol).cloned()
        };
        let market = market.ok_or_else(|| anyhow::anyhow!("Market not in registry: {}", slug))?;

        let condition_id = market.condition_id;
        let token_id = match side {
            TradeSide::Up => market.up_token,
            TradeSide::Down => market.down_token,
        };
        let side_label = side.as_str();
        let max_ask = self.config.strategy.max_ask_price;

        let current_ask = ask_store.read().await.get_ask(&token_id);
        let limit_price = match current_ask {
            Some(ask) if ask > 0.0 => {
                if ask > max_ask {
                    info!(
                        "⏭️ Skipping {} {} — ask ${:.4} above ${:.2} max",
                        symbol.to_uppercase(),
                        side_label,
                        ask,
                        max_ask
                    );
                    traded_markets.lock().await.release_pending(slug);
                    return Ok(());
                }
                let limit = round_limit_price(ask);
                info!(
                    "  Current {} ask: ${:.4} — limit buy @ ${:.2}",
                    side_label, ask, limit
                );
                limit
            }
            _ => {
                anyhow::bail!(
                    "No live ask for {} token — cannot place at current ask",
                    side_label
                );
            }
        };
        let size = self.config.strategy.trade_shares;
        let trade_usd = size * limit_price;
        let simulation = self.config.strategy.simulation_mode;

        let trade_id = tracker.lock().await.register_trade(
            slug.to_string(),
            symbol.clone(),
            side,
            condition_id.clone(),
            token_id.clone(),
            limit_price,
            size,
            trade_usd,
            simulation,
        );

        if simulation {
            info!(
                "[SIM] LIMIT BUY {} {} — {} shares @ ${:.2} (${:.2} notional) slug={}",
                side_label,
                symbol.to_uppercase(),
                size,
                limit_price,
                trade_usd,
                slug
            );
            info!(
                "[SIM] Would cancel order after {}s",
                self.config.strategy.cancel_after_secs
            );
            if let Some(path) = trades_log_path(&self.config) {
                spawn_trade_log(
                    &path,
                    tick,
                    price_change,
                    side,
                    true,
                    slug,
                    size,
                    limit_price,
                    trade_usd,
                    None,
                    current_ask,
                );
            }
        } else {
            let order = OrderRequest {
                token_id: token_id.clone(),
                side: "BUY".to_string(),
                size: format!("{:.2}", size),
                price: format!("{:.2}", limit_price),
                order_type: "GTC".to_string(),
            };

            let response = self.api.place_order(&order).await?;
            let order_id = response
                .order_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("No order_id returned"))?;

            info!(
                "✅ LIMIT BUY placed — {} {} @ ${:.2}, size {:.2}, order_id={}",
                symbol.to_uppercase(),
                side_label,
                limit_price,
                size,
                order_id
            );

            if let Some(path) = trades_log_path(&self.config) {
                spawn_trade_log(
                    &path,
                    tick,
                    price_change,
                    side,
                    false,
                    slug,
                    size,
                    limit_price,
                    trade_usd,
                    Some(&order_id),
                    current_ask,
                );
            }

            let api = self.api.clone();
            let cancel_secs = self.config.strategy.cancel_after_secs;
            let store = ask_store.clone();
            let tracker_clone = tracker.clone();
            let tid = token_id.clone();
            traded_markets.lock().await.mark_attempted(slug);
            tokio::spawn(async move {
                manage_live_order(
                    api,
                    store,
                    tracker_clone,
                    trade_id,
                    order_id,
                    tid,
                    limit_price,
                    cancel_secs,
                )
                .await;
            });
            return Ok(());
        }

        let cancel_secs = self.config.strategy.cancel_after_secs;
        let store = ask_store.clone();
        let tracker_clone = tracker.clone();
        let tid = token_id.clone();
        traded_markets.lock().await.mark_attempted(slug);
        tokio::spawn(async move {
            watch_order_fill(
                store,
                tracker_clone,
                trade_id,
                tid,
                limit_price,
                cancel_secs,
            )
            .await;
        });

        Ok(())
    }
}

fn round_limit_price(ask: f64) -> f64 {
    (ask * 100.0).round() / 100.0
}

fn trades_log_path(config: &Config) -> Option<PathBuf> {
    config
        .strategy
        .trades_log
        .as_ref()
        .map(|p| PathBuf::from(p))
}

fn spawn_trade_log(
    path: &PathBuf,
    tick: &PriceTick,
    price_change: f64,
    side: TradeSide,
    simulation: bool,
    slug: &str,
    size: f64,
    limit_price: f64,
    trade_usd: f64,
    order_id: Option<&str>,
    ask_at_signal: Option<f64>,
) {
    let path = path.clone();
    let tick = tick.clone();
    let slug = slug.to_string();
    let order_id = order_id.map(str::to_string);
    tokio::spawn(async move {
        let _ = tokio::task::spawn_blocking(move || {
            append_trade_log(
                &path,
                &tick,
                price_change,
                side,
                simulation,
                &slug,
                size,
                limit_price,
                trade_usd,
                order_id.as_deref(),
                ask_at_signal,
            )
        })
        .await;
    });
}

fn spawn_trade_log_failed(
    path: &PathBuf,
    tick: &PriceTick,
    price_change: f64,
    side: TradeSide,
    error: &str,
) {
    let path = path.clone();
    let tick = tick.clone();
    let error = error.to_string();
    tokio::spawn(async move {
        let _ = tokio::task::spawn_blocking(move || {
            append_trade_log_failed(&path, &tick, price_change, side, &error)
        })
        .await;
    });
}

fn append_trade_log(
    path: &PathBuf,
    tick: &PriceTick,
    price_change: f64,
    side: TradeSide,
    simulation: bool,
    slug: &str,
    size: f64,
    limit_price: f64,
    trade_usd: f64,
    order_id: Option<&str>,
    ask_at_signal: Option<f64>,
) -> Result<()> {
    let mode = if simulation { "SIM" } else { "LIVE" };
    let mut line = format!(
        "BUY [{mode}] {} {} {:.2} shares @ ${:.2} (${:.0} notional) slug={} spot=${:.4} change=${:+.4}",
        tick.symbol.to_uppercase(),
        side.as_str(),
        size,
        limit_price,
        trade_usd,
        slug,
        tick.price,
        price_change
    );
    if let Some(ask) = ask_at_signal {
        line.push_str(&format!(" ask=${ask:.4}"));
    }
    if let Some(oid) = order_id {
        line.push_str(&format!(" order_id={oid}"));
    }
    line.push_str(&format!(" @ {}", tick.timestamp_ms));

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{line}")?;
    Ok(())
}

fn append_trade_log_failed(
    path: &PathBuf,
    tick: &PriceTick,
    price_change: f64,
    side: TradeSide,
    error: &str,
) -> Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(
        file,
        "BUY [FAILED] {} {} spot=${:.4} change=${:+.4} error=\"{}\" @ {}",
        tick.symbol.to_uppercase(),
        side.as_str(),
        tick.price,
        price_change,
        error,
        tick.timestamp_ms
    )?;
    Ok(())
}
