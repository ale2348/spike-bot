//! Fill detection, resolution tracking, and PnL statistics.

use crate::market_prices::AskPriceStore;
use crate::models::TradeSide;
use log::{info, warn};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillStatus {
    Pending,
    Filled,
    Unfilled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    Win,
    Lose,
}

#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub id: u64,
    pub slug: String,
    pub symbol: String,
    pub side: TradeSide,
    pub condition_id: String,
    pub token_id: String,
    pub limit_price: f64,
    pub size: f64,
    pub cost_usd: f64,
    pub simulation: bool,
    pub fill_status: FillStatus,
    pub fill_ask: Option<f64>,
    pub resolution: Option<Resolution>,
    pub pnl: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct PerformanceStats {
    pub total_signals: u64,
    pub filled_trades: u64,
    pub unfilled_trades: u64,
    pub pending_resolution: u64,
    pub resolved_wins: u64,
    pub resolved_losses: u64,
    pub cumulative_pnl: f64,
}

impl PerformanceStats {
    pub fn fill_rate(&self) -> f64 {
        if self.total_signals == 0 {
            0.0
        } else {
            self.filled_trades as f64 / self.total_signals as f64
        }
    }

    pub fn win_rate(&self) -> f64 {
        let resolved = self.resolved_wins + self.resolved_losses;
        if resolved == 0 {
            0.0
        } else {
            self.resolved_wins as f64 / resolved as f64
        }
    }
}

pub struct PerformanceTracker {
    next_id: u64,
    trades: Vec<TradeRecord>,
    stats: PerformanceStats,
    log_path: Option<PathBuf>,
}

impl PerformanceTracker {
    pub fn new(log_path: Option<PathBuf>) -> Self {
        Self {
            next_id: 1,
            trades: Vec::new(),
            stats: PerformanceStats::default(),
            log_path,
        }
    }

    pub fn stats(&self) -> &PerformanceStats {
        &self.stats
    }

    pub fn record_signal(&mut self) {
        self.stats.total_signals += 1;
    }

    pub fn register_trade(
        &mut self,
        slug: String,
        symbol: String,
        side: TradeSide,
        condition_id: String,
        token_id: String,
        limit_price: f64,
        size: f64,
        cost_usd: f64,
        simulation: bool,
    ) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.trades.push(TradeRecord {
            id,
            slug,
            symbol,
            side,
            condition_id,
            token_id,
            limit_price,
            size,
            cost_usd,
            simulation,
            fill_status: FillStatus::Pending,
            fill_ask: None,
            resolution: None,
            pnl: None,
        });
        id
    }

    pub fn mark_filled(&mut self, trade_id: u64, ask: f64) {
        let log_line = {
            let Some(trade) = self.trades.iter_mut().find(|t| t.id == trade_id) else {
                return;
            };
            if trade.fill_status != FillStatus::Pending {
                return;
            }
            trade.fill_status = FillStatus::Filled;
            trade.fill_ask = Some(ask);
            self.stats.filled_trades += 1;
            self.stats.pending_resolution += 1;

            let mode = if trade.simulation { "SIM" } else { "LIVE" };
            info!(
                "✅ FILL [{mode}] {} {} — ask ${:.4} <= limit ${:.2} (slug={})",
                trade.symbol.to_uppercase(),
                trade.side.as_str(),
                ask,
                trade.limit_price,
                trade.slug
            );
            format!(
                "FILL id={} {} {} ask=${:.4} limit=${:.2} slug={}",
                trade.id,
                trade.symbol.to_uppercase(),
                trade.side.as_str(),
                ask,
                trade.limit_price,
                trade.slug
            )
        };
        let _ = self.append_log_async(&log_line);
        self.log_summary();
    }

    pub fn mark_unfilled(&mut self, trade_id: u64, last_ask: Option<f64>) {
        let log_line = {
            let Some(trade) = self.trades.iter_mut().find(|t| t.id == trade_id) else {
                return;
            };
            if trade.fill_status != FillStatus::Pending {
                return;
            }
            trade.fill_status = FillStatus::Unfilled;
            trade.fill_ask = last_ask;
            self.stats.unfilled_trades += 1;

            let ask_msg = last_ask
                .map(|a| format!("last ask ${:.4}", a))
                .unwrap_or_else(|| "no ask".to_string());
            info!(
                "❌ NO FILL {} {} — {} > limit ${:.2} (slug={})",
                trade.symbol.to_uppercase(),
                trade.side.as_str(),
                ask_msg,
                trade.limit_price,
                trade.slug
            );
            format!(
                "NO_FILL id={} {} {} {} limit=${:.2} slug={}",
                trade.id,
                trade.symbol.to_uppercase(),
                trade.side.as_str(),
                ask_msg,
                trade.limit_price,
                trade.slug
            )
        };
        let _ = self.append_log_async(&log_line);
        self.log_summary();
    }

    pub fn resolve_trade(&mut self, trade_id: u64, won: bool) {
        let log_line = {
            let Some(trade) = self.trades.iter_mut().find(|t| t.id == trade_id) else {
                return;
            };
            if trade.fill_status != FillStatus::Filled || trade.resolution.is_some() {
                return;
            }

            let pnl = if won {
                trade.size - trade.cost_usd
            } else {
                -trade.cost_usd
            };
            trade.resolution = Some(if won { Resolution::Win } else { Resolution::Lose });
            trade.pnl = Some(pnl);
            self.stats.pending_resolution = self.stats.pending_resolution.saturating_sub(1);
            if won {
                self.stats.resolved_wins += 1;
            } else {
                self.stats.resolved_losses += 1;
            }
            self.stats.cumulative_pnl += pnl;

            let outcome = if won { "WIN" } else { "LOSE" };
            info!(
                "🏁 RESOLVED {} {} — {} PnL ${:+.4} (cumulative ${:+.4}) slug={}",
                trade.symbol.to_uppercase(),
                trade.side.as_str(),
                outcome,
                pnl,
                self.stats.cumulative_pnl,
                trade.slug
            );
            format!(
                "RESOLVED id={} {} {} {} pnl=${:+.4} cumulative=${:+.4} slug={}",
                trade.id,
                trade.symbol.to_uppercase(),
                trade.side.as_str(),
                outcome,
                pnl,
                self.stats.cumulative_pnl,
                trade.slug
            )
        };
        let _ = self.append_log_async(&log_line);
        self.log_summary();
    }

    fn log_summary(&self) {
        let s = &self.stats;
        info!(
            "📊 Stats — signals: {}, fill rate: {:.1}% ({}/{}), win rate: {:.1}% ({}/{}), PnL: ${:+.4}",
            s.total_signals,
            s.fill_rate() * 100.0,
            s.filled_trades,
            s.total_signals,
            s.win_rate() * 100.0,
            s.resolved_wins,
            s.resolved_wins + s.resolved_losses,
            s.cumulative_pnl
        );
    }

    fn append_log_async(&self, line: &str) {
        let Some(path) = self.log_path.clone() else {
            return;
        };
        let line = line.to_string();
        tokio::spawn(async move {
            let _ = tokio::task::spawn_blocking(move || append_log_sync(&path, &line)).await;
        });
    }

    pub fn filled_pending_resolution(&self) -> Vec<(u64, String)> {
        self.trades
            .iter()
            .filter(|t| t.fill_status == FillStatus::Filled && t.resolution.is_none())
            .map(|t| (t.id, t.condition_id.clone()))
            .collect()
    }

    pub fn token_id_for(&self, trade_id: u64) -> Option<String> {
        self.trades
            .iter()
            .find(|t| t.id == trade_id)
            .map(|t| t.token_id.clone())
    }

    pub fn is_filled(&self, trade_id: u64) -> bool {
        self.trades
            .iter()
            .find(|t| t.id == trade_id)
            .is_some_and(|t| t.fill_status == FillStatus::Filled)
    }
}

fn append_log_sync(path: &PathBuf, line: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{line}")?;
    Ok(())
}

/// Returns true if a limit buy at `limit_price` would fill against `best_ask`.
pub fn would_fill_at_ask(best_ask: f64, limit_price: f64) -> bool {
    best_ask > 0.0 && best_ask <= limit_price
}

/// Poll ask prices during the order window; fill status is tracked for performance only.
pub async fn watch_order_fill(
    store: Arc<RwLock<AskPriceStore>>,
    tracker: Arc<Mutex<PerformanceTracker>>,
    trade_id: u64,
    token_id: String,
    limit_price: f64,
    cancel_secs: u64,
) {
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(cancel_secs);
    let mut last_ask: Option<f64> = None;

    loop {
        {
            let ask = store.read().await.get_ask(&token_id);
            if let Some(ask) = ask {
                last_ask = Some(ask);
                if would_fill_at_ask(ask, limit_price) {
                    tracker.lock().await.mark_filled(trade_id, ask);
                    return;
                }
            }
        }

        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    tracker.lock().await.mark_unfilled(trade_id, last_ask);
}

/// Watch a live order for fill; cancel only if still unfilled after the window.
pub async fn manage_live_order(
    api: Arc<crate::api::PolymarketApi>,
    store: Arc<RwLock<AskPriceStore>>,
    tracker: Arc<Mutex<PerformanceTracker>>,
    trade_id: u64,
    order_id: String,
    token_id: String,
    limit_price: f64,
    cancel_secs: u64,
) {
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(cancel_secs);
    let mut last_ask: Option<f64> = None;

    loop {
        {
            let ask = store.read().await.get_ask(&token_id);
            if let Some(ask) = ask {
                last_ask = Some(ask);
                if would_fill_at_ask(ask, limit_price) {
                    tracker.lock().await.mark_filled(trade_id, ask);
                    info!("Order {order_id} filled — not cancelling");
                    return;
                }
            }
        }

        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    if tracker.lock().await.is_filled(trade_id) {
        return;
    }

    tracker.lock().await.mark_unfilled(trade_id, last_ask);

    match api.cancel_order(&order_id).await {
        Ok(()) => info!("🗑️ Cancelled unfilled order {order_id} after {cancel_secs}s"),
        Err(e) => warn!("Failed to cancel order {order_id}: {e}"),
    }
}

/// Periodically check market resolution for filled trades.
pub async fn run_resolution_poller(
    api: Arc<crate::api::PolymarketApi>,
    tracker: Arc<Mutex<PerformanceTracker>>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        let pending: Vec<(u64, String)> = tracker.lock().await.filled_pending_resolution();
        for (trade_id, condition_id) in pending {
            let token_id = tracker.lock().await.token_id_for(trade_id);
            let Some(token_id) = token_id else {
                continue;
            };

            match api.get_market_resolution(&condition_id).await {
                Ok(Some(winner_token_id)) => {
                    let won = winner_token_id == token_id;
                    tracker.lock().await.resolve_trade(trade_id, won);
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(
                        "Resolution check failed for trade {} ({}): {}",
                        trade_id, condition_id, e
                    );
                }
            }
        }
    }
}
