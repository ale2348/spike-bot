//! Tracks markets where an order was placed (persisted) and in-flight placements (memory only).

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct TradedMarketsCache {
    /// Markets where we placed an order this period (filled or not).
    attempted: HashSet<String>,
    /// Markets with an order being placed right now.
    pending: HashSet<String>,
    path: PathBuf,
}

impl TradedMarketsCache {
    pub fn new(cache_dir: &str) -> Self {
        let path = PathBuf::from(cache_dir).join("traded_markets.cache");
        let mut attempted = load_traded_slugs(&path);
        prune_expired_slugs(&mut attempted);
        Self {
            attempted,
            pending: HashSet::new(),
            path,
        }
    }

    pub fn is_blocked(&self, slug: &str) -> bool {
        self.attempted.contains(slug) || self.pending.contains(slug)
    }

    pub fn mark_pending(&mut self, slug: &str) {
        self.pending.insert(slug.to_string());
    }

    pub fn release_pending(&mut self, slug: &str) {
        self.pending.remove(slug);
    }

    /// Mark a market as attempted in memory immediately; persist to disk in the background.
    pub fn mark_attempted(&mut self, slug: &str) {
        self.pending.remove(slug);
        if self.attempted.insert(slug.to_string()) {
            let path = self.path.clone();
            let slug = slug.to_string();
            tokio::spawn(async move {
                let _ = tokio::task::spawn_blocking(move || persist_slug(&path, &slug)).await;
            });
        }
    }
}

fn persist_slug(path: &Path, slug: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{slug}")?;
    Ok(())
}

fn load_traded_slugs(path: &Path) -> HashSet<String> {
    if !path.exists() {
        return HashSet::new();
    }
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return HashSet::new(),
    };
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

fn slug_period_end(slug: &str) -> Option<i64> {
    let ts: i64 = slug.rsplit('-').next()?.parse().ok()?;
    Some(ts + 5 * 60)
}

fn prune_expired_slugs(slugs: &mut HashSet<String>) {
    let now = chrono::Utc::now().timestamp();
    slugs.retain(|slug| slug_period_end(slug).is_some_and(|end| end > now));
}
