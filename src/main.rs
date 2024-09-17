use futures_util::StreamExt;
use serde_json::Value;
use tokio_tungstenite::connect_async;
use chrono::{DateTime, Utc, Local, TimeZone};
use anyhow::{Result, anyhow};
use std::sync::Arc;

trait KlineObserver: Send + Sync {
    fn on_new_kline(&self, kline_data: &KlineData);
}

struct WebSocketSubject {
    observer: Arc<dyn KlineObserver>,
}

impl WebSocketSubject {
    fn new(observer: Arc<dyn KlineObserver>) -> Self {
        WebSocketSubject { observer }
    }

    async fn run(&self, symbol: &str, interval: &str) -> Result<()> {
        let ws_url = format!(
            "wss://stream.binance.com:9443/ws/{}@kline_{}",
            symbol, interval
        );

        println!("Connecting to Binance WebSocket for {}...", symbol);
        let (ws_stream, _) = connect_async(&ws_url).await?;
        println!("Connected to WebSocket.");

        let (_, mut read) = ws_stream.split();

        while let Some(Ok(message)) = read.next().await {
            if let Ok(text) = message.to_text() {
                let json: Value = serde_json::from_str(text)?;
                if let Some(_kline) = json["k"].as_object() {
                    let kline_data = KlineData::new(&json["k"])?;
                    self.observer.on_new_kline(&kline_data);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct KlineData {
    interval_start: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl KlineData {
    fn new(kline: &Value) -> Result<Self> {
        Ok(Self {
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
    let timestamp = kline["t"].as_i64()
        .ok_or_else(|| anyhow!("Invalid timestamp"))?;
    Utc.timestamp_millis_opt(timestamp)
        .single()
        .ok_or_else(|| anyhow!("Invalid timestamp"))
}

fn parse_price(kline: &Value, key: &str) -> Result<f64> {
    kline[key].as_str()
        .ok_or_else(|| anyhow!("Invalid {} price", key))?
        .parse()
        .map_err(|_| anyhow!("Failed to parse {} price", key))
}

fn parse_volume(kline: &Value) -> Result<f64> {
    kline["v"].as_str()
        .ok_or_else(|| anyhow!("Invalid volume"))?
        .parse()
        .map_err(|_| anyhow!("Failed to parse volume"))
}

struct Logger;

impl KlineObserver for Logger {
    fn on_new_kline(&self, kline_data: &KlineData) {
        let local_time = Local::now();
        println!(
            "Local time: {} | Interval start: {} | Open: {:.2} | High: {:.2} | \
             Low: {:.2} | Close: {:.2} | Volume: {:.2} | Change: {:.2} ({:.2}%)",
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let symbol = "btcusdt";
    let interval = "1m";

    let logger = Arc::new(Logger);
    let subject = WebSocketSubject::new(logger);

    subject.run(symbol, interval).await
}