use reqwest;
use serde_json::Value;
use chrono::{DateTime, Utc, TimeZone, Local};
use tokio::time::{sleep, Instant};
use std::time::Duration;
use anyhow::{Result, anyhow};  // Improved error handling with anyhow

#[derive(Debug, Clone)]
struct KlineData {
    interval_start: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl KlineData {
    fn new(kline: &[Value]) -> Result<Self> {
        Ok(Self {
            interval_start: Utc.timestamp_millis_opt(kline[0].as_i64().ok_or_else(|| anyhow!("Failed to parse interval start"))?).unwrap(),
            open: kline[1].as_str().ok_or_else(|| anyhow!("Failed to parse open value"))?.parse().map_err(|_| anyhow!("Invalid open value"))?,
            high: kline[2].as_str().ok_or_else(|| anyhow!("Failed to parse high value"))?.parse().map_err(|_| anyhow!("Invalid high value"))?,
            low: kline[3].as_str().ok_or_else(|| anyhow!("Failed to parse low value"))?.parse().map_err(|_| anyhow!("Invalid low value"))?,
            close: kline[4].as_str().ok_or_else(|| anyhow!("Failed to parse close value"))?.parse().map_err(|_| anyhow!("Invalid close value"))?,
            volume: kline[5].as_str().ok_or_else(|| anyhow!("Failed to parse volume value"))?.parse().map_err(|_| anyhow!("Invalid volume value"))?,
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
    let symbol = "BTCUSDT";
    let interval = "1m";

    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval={}&limit=1",
        symbol, interval
    );

    let client = reqwest::Client::new();

    println!("Starting comprehensive high-frequency data retrieval for {}...", symbol);
    println!("Press Ctrl+C to stop the program.");

    let mut last_kline: Option<KlineData> = None;
    let mut requests_count = 0;
    let mut updates_count = 0;
    let start_time = Instant::now();

    loop {
        match client.get(&url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let body: Value = response.json().await?;

                    if let Some(klines) = body.as_array() {
                        if let Some(kline) = klines.first() {
                            if let Some(kline_array) = kline.as_array() {
                                if let Ok(kline_data) = KlineData::new(kline_array) {
                                    let local_time = Local::now();

                                    if last_kline.as_ref().map_or(true, |last| last.close != kline_data.close) {
                                        updates_count += 1;
                                        let is_new_interval = last_kline.as_ref().map_or(true, |last| last.interval_start != kline_data.interval_start);

                                        if is_new_interval {
                                            println!("\nNew interval starting:");
                                            updates_count = 1;
                                        }

                                        println!(
                                            "Local time: {} | Interval start: {} | Open: {:.2} | High: {:.2} | Low: {:.2} | Close: {:.2} | Volume: {:.2} | Change: {:.2} ({:.2}%) | Updates: {}",
                                            local_time.format("%Y-%m-%d %H:%M:%S"),
                                            kline_data.interval_start.format("%Y-%m-%d %H:%M"),
                                            kline_data.open,
                                            kline_data.high,
                                            kline_data.low,
                                            kline_data.close,
                                            kline_data.volume,
                                            kline_data.price_change(),
                                            kline_data.price_change_percent(),
                                            updates_count
                                        );

                                        last_kline = Some(kline_data);
                                    }
                                } else {
                                    println!("Error parsing Kline data");
                                }
                            }
                        }
                    }
                } else {
                    println!("Error: {}", response.status());
                }
            }
            Err(err) => {
                println!("Request failed: {}", err);
            }
        }

        requests_count += 1;
        if requests_count % 100 == 0 {
            let elapsed = start_time.elapsed().as_secs_f64();
            println!("\nAverage request rate: {:.2} requests/second", requests_count as f64 / elapsed);
        }

        // Adjust the sleep interval dynamically if needed based on the rate limits
        sleep(Duration::from_millis(100)).await;
    }
}
