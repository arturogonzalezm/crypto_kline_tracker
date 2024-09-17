use anyhow::{anyhow, Result};
use chrono::{DateTime, Local, TimeZone, Utc};
use futures_util::StreamExt;
use log::{debug, error, info, warn};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;

#[derive(Debug, Clone)]
struct KlineData {
    symbol: String,
    interval: String,
    interval_start: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl KlineData {
    fn new(symbol: String, interval: String, kline: &Value) -> Result<Self> {
        Ok(Self {
            symbol,
            interval,
            interval_start: parse_timestamp(kline)?,
            open: parse_price(kline, "o")?,
            high: parse_price(kline, "h")?,
            low: parse_price(kline, "l")?,
            close: parse_price(kline, "c")?,
            volume: parse_volume(kline)?,
        })
    }

    fn price_change(&self) -> f64 {
        self.close - self.open
    }

    fn price_change_percent(&self) -> f64 {
        (self.price_change() / self.open) * 100.0
    }
}

fn parse_timestamp(kline: &Value) -> Result<DateTime<Utc>> {
    let timestamp = kline["t"]
        .as_i64()
        .ok_or_else(|| anyhow!("Invalid timestamp"))?;
    Utc.timestamp_millis_opt(timestamp)
        .single()
        .ok_or_else(|| anyhow!("Invalid timestamp"))
}

fn parse_price(kline: &Value, key: &str) -> Result<f64> {
    kline[key]
        .as_str()
        .ok_or_else(|| anyhow!("Invalid {} price", key))?
        .parse()
        .map_err(|_| anyhow!("Failed to parse {} price", key))
}

fn parse_volume(kline: &Value) -> Result<f64> {
    kline["v"]
        .as_str()
        .ok_or_else(|| anyhow!("Invalid volume"))?
        .parse()
        .map_err(|_| anyhow!("Failed to parse volume"))
}

async fn run_websocket(
    symbol: String,
    interval: String,
    tx: mpsc::Sender<KlineData>,
) -> Result<()> {
    let ws_url = format!(
        "wss://stream.binance.com:9443/ws/{}@kline_{}",
        symbol, interval
    );

    info!("Connecting to Binance WebSocket for {} {}...", symbol, interval);
    let (ws_stream, _) = connect_async(&ws_url).await?;
    info!("Connected to WebSocket for {} {}.", symbol, interval);

    let (_, mut read) = ws_stream.split();

    while let Some(Ok(message)) = read.next().await {
        if let Ok(text) = message.to_text() {
            let json: Value = serde_json::from_str(text)?;
            if let Some(_kline) = json["k"].as_object() {
                let kline_data =
                    KlineData::new(symbol.clone(), interval.clone(), &json["k"])?;
                tx.send(kline_data).await?;
                debug!("Sent kline data for {} {}", symbol, interval);
            }
        }
    }
    warn!("WebSocket connection closed for {} {}", symbol, interval);
    Ok(())
}

fn process_kline_data(kline_data: &KlineData) {
    let local_time = Local::now();
    info!(
        "Symbol: {} | Interval: {} | Local time: {} | Interval start: {} | \
         Open: {:.2} | High: {:.2} | Low: {:.2} | Close: {:.2} | \
         Volume: {:.2} | Change: {:.2} ({:.2}%)",
        kline_data.symbol,
        kline_data.interval,
        local_time.format("%Y-%m-%d %H:%M:%S"),
        kline_data.interval_start.format("%Y-%m-%d %H:%M"),
        kline_data.open,
        kline_data.high,
        kline_data.low,
        kline_data.close,
        kline_data.volume,
        kline_data.price_change(),
        kline_data.price_change_percent(),
    );
}

fn spawn_websocket_tasks(
    symbols: &[&str],
    intervals: &[&str],
    tx: mpsc::Sender<KlineData>,
) -> Vec<tokio::task::JoinHandle<()>> {
    symbols
        .iter()
        .flat_map(|&symbol| {
            let tx = tx.clone();
            intervals.iter().map(move |&interval| {
                let symbol = symbol.to_string();
                let interval = interval.to_string();
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = run_websocket(symbol.clone(), interval.clone(), tx).await {
                        error!("WebSocket error for {} {}: {}", symbol, interval, e);
                    }
                })
            })
        })
        .collect()
}

async fn process_kline_stream(mut rx: mpsc::Receiver<KlineData>) {
    let mut kline_cache: HashMap<(String, String), KlineData> = HashMap::new();

    while let Some(kline_data) = rx.recv().await {
        kline_cache.insert(
            (kline_data.symbol.clone(), kline_data.interval.clone()),
            kline_data,
        );

        kline_cache.par_iter().for_each(|(_, data)| {
            process_kline_data(data);
        });

        let avg_price_change: f64 = kline_cache
            .values()
            .map(|data| data.price_change_percent())
            .sum::<f64>()
            / kline_cache.len() as f64;

        info!("Average price change across all symbols: {:.2}%", avg_price_change);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let symbols = &["btcusdt", "ethusdt", "bnbusdt", "adausdt", "dogeusdt"];
    let intervals = &["1m", "5m", "15m"];

    info!("Starting Binance WebSocket client");
    debug!("Symbols: {:?}, Intervals: {:?}", symbols, intervals);

    let (tx, rx) = mpsc::channel(100);

    let tasks = spawn_websocket_tasks(symbols, intervals, tx);
    let processor = tokio::spawn(process_kline_stream(rx));

    for task in tasks {
        task.await?;
    }

    processor.await?;

    info!("Binance WebSocket client shutting down");
    Ok(())
}