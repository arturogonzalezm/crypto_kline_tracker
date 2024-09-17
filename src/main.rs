use futures_util::{StreamExt};
use serde_json::Value;
use tokio_tungstenite::connect_async;
use chrono::{DateTime, Utc, Local, TimeZone};  // Import TimeZone for timestamp methods
use anyhow::{Result, anyhow};  // For improved error handling

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
    // Improved error handling using `Result`
    fn new(kline: &Value) -> Result<Self> {
        let interval_start = match Utc.timestamp_millis_opt(kline["t"].as_i64().ok_or_else(|| anyhow!("Invalid timestamp"))?) {
            chrono::LocalResult::Single(datetime) => datetime,
            _ => Utc::now(), // Fallback to current time if parsing fails
        };

        Ok(Self {
            interval_start,
            open: kline["o"].as_str().ok_or_else(|| anyhow!("Invalid open price"))?.parse().map_err(|_| anyhow!("Failed to parse open price"))?,
            high: kline["h"].as_str().ok_or_else(|| anyhow!("Invalid high price"))?.parse().map_err(|_| anyhow!("Failed to parse high price"))?,
            low: kline["l"].as_str().ok_or_else(|| anyhow!("Invalid low price"))?.parse().map_err(|_| anyhow!("Failed to parse low price"))?,
            close: kline["c"].as_str().ok_or_else(|| anyhow!("Invalid close price"))?.parse().map_err(|_| anyhow!("Failed to parse close price"))?,
            volume: kline["v"].as_str().ok_or_else(|| anyhow!("Invalid volume"))?.parse().map_err(|_| anyhow!("Failed to parse volume"))?,
        })
    }

    fn price_change(&self) -> f64 {
        self.close - self.open
    }

    fn price_change_percent(&self) -> f64 {
        (self.price_change() / self.open) * 100.0
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let symbol = "btcusdt";
    let interval = "1m";
    let ws_url = format!(
        "wss://stream.binance.com:9443/ws/{}@kline_{}",
        symbol, interval
    );

    println!("Connecting to Binance WebSocket for {}...", symbol);

    // Connect to WebSocket and ensure no unnecessary delays
    let (ws_stream, _) = connect_async(ws_url.as_str()).await?;
    println!("Connected to WebSocket.");

    let (mut _write, mut read) = ws_stream.split();

    while let Some(Ok(message)) = read.next().await {
        if let Ok(text) = message.to_text() {
            let json: Value = serde_json::from_str(text)?;

            if let Some(_kline) = json["k"].as_object() {
                match KlineData::new(&json["k"]) {
                    Ok(kline_data) => {
                        let local_time = Local::now();

                        // Ensure minimal delay in processing and logging
                        println!(
                            "Local time: {} | Interval start: {} | Open: {:.2} | High: {:.2} | Low: {:.2} | Close: {:.2} | Volume: {:.2} | Change: {:.2} ({:.2}%)",
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
                    Err(e) => {
                        eprintln!("Error parsing kline data: {}", e);
                    }
                }
            }
        }
    }

    Ok(())
}
