use crate::api::PolymarketApi;
use anyhow::Result;
use chrono::{TimeZone, Timelike};
use chrono_tz::America::New_York;
use std::sync::Arc;

/// Polymarket aligns 5m markets to Eastern Time (ET). Period start = start of current window in ET, as Unix timestamp.
fn period_start_et_unix(minutes: i64) -> i64 {
    let utc_now = chrono::Utc::now();
    let et = New_York;
    let now_et = utc_now.with_timezone(&et);
    let minute_floor = (now_et.minute() as i64 / minutes) * minutes;
    let truncated_naive = now_et
        .date_naive()
        .and_hms_opt(now_et.hour(), minute_floor as u32, 0)
        .unwrap();
    let dt_et = et
        .from_local_datetime(&truncated_naive)
        .single()
        .or_else(|| et.from_local_datetime(&truncated_naive).earliest())
        .expect("ET period start");
    dt_et.timestamp()
}

/// 5m slug: {symbol}-updown-5m-{timestamp} (e.g. btc, eth, sol, xrp).
pub fn build_5m_slug(symbol: &str, period_start_unix: i64) -> String {
    format!("{}-updown-5m-{}", symbol.to_lowercase(), period_start_unix)
}

/// Current 5-minute period start (Unix). Aligned to 5m boundaries in Eastern Time.
pub fn current_5m_period_start() -> i64 {
    period_start_et_unix(5)
}

/// Seconds until the 5m window that began at `period_start_unix` ends (`period_start + 300s`).
pub fn seconds_remaining_in_5m_period(period_start_unix: i64) -> i64 {
    let end = period_start_unix + 5 * 60;
    let now = chrono::Utc::now().timestamp();
    (end - now).max(0)
}

/// Countdown for logs, e.g. `3m 22s`.
pub fn fmt_5m_period_remaining(period_start_unix: i64) -> String {
    let s = seconds_remaining_in_5m_period(period_start_unix);
    let m = s / 60;
    let sec = s % 60;
    format!("{}m {}s", m, sec)
}

pub struct MarketDiscovery {
    api: Arc<PolymarketApi>,
}

impl Clone for MarketDiscovery {
    fn clone(&self) -> Self {
        Self {
            api: self.api.clone(),
        }
    }
}

impl MarketDiscovery {
    pub fn new(api: Arc<PolymarketApi>) -> Self {
        Self { api }
    }

    pub async fn get_market_tokens(&self, condition_id: &str) -> Result<(String, String)> {
        let details = self.api.get_market(condition_id).await?;
        let mut up_token = None;
        let mut down_token = None;

        for token in details.tokens {
            let outcome = token.outcome.to_uppercase();
            if outcome.contains("UP") || outcome == "1" {
                up_token = Some(token.token_id);
            } else if outcome.contains("DOWN") || outcome == "0" {
                down_token = Some(token.token_id);
            }
        }

        let up = up_token.ok_or_else(|| anyhow::anyhow!("Up token not found"))?;
        let down = down_token.ok_or_else(|| anyhow::anyhow!("Down token not found"))?;

        Ok((up, down))
    }

    pub async fn get_5m_market(&self, symbol: &str, period_start: i64) -> Result<Option<String>> {
        let slug = build_5m_slug(symbol, period_start);
        let market = match self.api.get_market_by_slug(&slug).await {
            Ok(m) => m,
            Err(_) => return Ok(None),
        };
        if !market.active || market.closed {
            return Ok(None);
        }
        Ok(Some(market.condition_id))
    }
}
