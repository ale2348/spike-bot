use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value = "config.json")]
    pub config: PathBuf,

    #[arg(long)]
    pub redeem: bool,

    #[arg(long, requires = "redeem")]
    pub condition_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub polymarket: PolymarketConfig,
    pub strategy: StrategyConfig,
}

/// Per-symbol momentum threshold: price must move by at least `price_change_usd` (up or down) within `lookback_secs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolMomentumConfig {
    pub price_change_usd: f64,
    #[serde(default = "default_lookback_secs")]
    pub lookback_secs: u64,
}

/// Momentum strategy: Binance price feed → signal → limit buy Up/Down @ current ask → cancel after N seconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    #[serde(default = "default_symbols")]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub simulation_mode: bool,

    /// Per-symbol momentum thresholds. Keys: `btc`, `eth`, `sol`, `xrp`.
    #[serde(default = "default_momentum_thresholds")]
    pub momentum: HashMap<String, SymbolMomentumConfig>,

    /// Unused in this bot variant — limit price is taken from the current Polymarket ask.
    #[serde(default = "default_limit_price")]
    pub limit_price: f64,

    /// Fixed number of shares per limit order (Polymarket minimum is 5).
    #[serde(default = "default_trade_shares")]
    pub trade_shares: f64,

    /// Unused in this bot variant — notional is `trade_shares * limit_price`.
    #[serde(default = "default_trade_amount_usd")]
    pub trade_amount_usd: f64,

    /// Cancel limit order after this many seconds, regardless of fill.
    #[serde(default = "default_cancel_after_secs")]
    pub cancel_after_secs: u64,

    /// Skip trade when Polymarket ask exceeds this price.
    #[serde(default = "default_max_ask_price")]
    pub max_ask_price: f64,

    /// Minimum seconds between signals for the same symbol.
    #[serde(default = "default_signal_cooldown_secs")]
    pub signal_cooldown_secs: u64,

    /// Append buy events to this file (e.g. `trades.log`). Omit to log trades to terminal only.
    #[serde(default)]
    pub trades_log: Option<String>,

    /// Poll interval for checking filled-trade market resolutions (seconds).
    #[serde(default = "default_resolution_check_secs")]
    pub resolution_check_secs: u64,

    /// Append fill/resolution/PnL events (e.g. `performance.log`).
    #[serde(default = "default_performance_log")]
    pub performance_log: Option<String>,
}

fn default_resolution_check_secs() -> u64 {
    30
}

fn default_performance_log() -> Option<String> {
    Some("performance.log".to_string())
}

fn default_symbols() -> Vec<String> {
    vec![
        "btc".into(),
        "eth".into(),
        "sol".into(),
        "xrp".into(),
    ]
}

fn default_lookback_secs() -> u64 {
    60
}

fn default_limit_price() -> f64 {
    0.50
}

fn default_trade_amount_usd() -> f64 {
    30.0
}

fn default_trade_shares() -> f64 {
    5.0
}

fn default_cancel_after_secs() -> u64 {
    4
}

fn default_max_ask_price() -> f64 {
    0.98
}

fn default_signal_cooldown_secs() -> u64 {
    60
}

fn default_momentum_thresholds() -> HashMap<String, SymbolMomentumConfig> {
    let mut m = HashMap::new();
    m.insert(
        "btc".into(),
        SymbolMomentumConfig {
            price_change_usd: 120.0,
            lookback_secs: 60,
        },
    );
    m.insert(
        "eth".into(),
        SymbolMomentumConfig {
            price_change_usd: 15.0,
            lookback_secs: 60,
        },
    );
    m.insert(
        "sol".into(),
        SymbolMomentumConfig {
            price_change_usd: 4.0,
            lookback_secs: 60,
        },
    );
    m.insert(
        "xrp".into(),
        SymbolMomentumConfig {
            price_change_usd: 0.05,
            lookback_secs: 60,
        },
    );
    m
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketConfig {
    pub gamma_api_url: String,
    pub clob_api_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
    pub private_key: Option<String>,
    pub proxy_wallet_address: Option<String>,
    pub signature_type: Option<u8>,
    #[serde(default)]
    pub rpc_url: Option<String>,
    #[serde(default = "default_ws_url")]
    pub ws_url: String,
}

fn default_ws_url() -> String {
    "wss://ws-subscriptions-clob.polymarket.com".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            polymarket: PolymarketConfig {
                gamma_api_url: "https://gamma-api.polymarket.com".to_string(),
                clob_api_url: "https://clob.polymarket.com".to_string(),
                api_key: None,
                api_secret: None,
                api_passphrase: None,
                private_key: None,
                proxy_wallet_address: None,
                signature_type: None,
                rpc_url: None,
                ws_url: default_ws_url(),
            },
            strategy: StrategyConfig {
                symbols: default_symbols(),
                simulation_mode: false,
                momentum: default_momentum_thresholds(),
                limit_price: default_limit_price(),
                trade_shares: default_trade_shares(),
                trade_amount_usd: default_trade_amount_usd(),
                cancel_after_secs: default_cancel_after_secs(),
                max_ask_price: default_max_ask_price(),
                signal_cooldown_secs: default_signal_cooldown_secs(),
                trades_log: None,
                resolution_check_secs: default_resolution_check_secs(),
                performance_log: default_performance_log(),
            },
        }
    }
}

impl Config {
    pub fn load(path: &PathBuf) -> anyhow::Result<Self> {
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            Ok(serde_json::from_str(&content)?)
        } else {
            let config = Config::default();
            let content = serde_json::to_string_pretty(&config)?;
            std::fs::write(path, content)?;
            Ok(config)
        }
    }

    pub fn momentum_for(&self, symbol: &str) -> Option<&SymbolMomentumConfig> {
        self.strategy.momentum.get(&symbol.to_lowercase())
    }
}
