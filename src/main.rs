use reqwest;
use serde_json::Value;
use chrono::{DateTime, Utc, TimeZone, Local};
use tokio::time::{sleep, Instant};
use std::time::Duration;

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
    fn new(kline: &[Value]) -> Self {
        Self {
            interval_start: Utc.timestamp_millis_opt(kline[0].as_i64().unwrap()).unwrap(),
            open: kline[1].as_str().unwrap().parse().unwrap(),
            high: kline[2].as_str().unwrap().parse().unwrap(),
            low: kline[3].as_str().unwrap().parse().unwrap(),
            close: kline[4].as_str().unwrap().parse().unwrap(),
            volume: kline[5].as_str().unwrap().parse().unwrap(),
        }
    }

    fn price_change(&self) -> f64 {
        self.close - self.open
    }

    fn price_change_percent(&self) -> f64 {
        (self.price_change() / self.open) * 100.0
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        let response = client.get(&url).send().await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;

            if let Some(klines) = body.as_array() {
                if let Some(kline) = klines.first() {
                    if let Some(kline_data) = kline.as_array() {
                        let current_kline = KlineData::new(kline_data);
                        let local_time = Local::now();

                        if last_kline.as_ref().map_or(true, |last| last.close != current_kline.close) {
                            updates_count += 1;
                            let is_new_interval = last_kline.as_ref().map_or(true, |last| last.interval_start != current_kline.interval_start);

                            if is_new_interval {
                                println!("\nNew interval starting:");
                                updates_count = 1;
                            }

                            println!(
                                "Local time: {} | Interval start: {} | Open: {:.2} | High: {:.2} | Low: {:.2} | Close: {:.2} | Volume: {:.2} | Change: {:.2} ({:.2}%) | Updates: {}",
                                local_time.format("%Y-%m-%d %H:%M:%S"),
                                current_kline.interval_start.format("%Y-%m-%d %H:%M"),
                                current_kline.open,
                                current_kline.high,
                                current_kline.low,
                                current_kline.close,
                                current_kline.volume,
                                current_kline.price_change(),
                                current_kline.price_change_percent(),
                                updates_count
                            );

                            last_kline = Some(current_kline);
                        }
                    }
                }
            }
        } else {
            println!("Error: {}", response.status());
        }

        requests_count += 1;
        if requests_count % 100 == 0 {
            let elapsed = start_time.elapsed().as_secs_f64();
            println!("\nAverage request rate: {:.2} requests/second", requests_count as f64 / elapsed);
        }

        // Wait for 100 milliseconds before the next request
        sleep(Duration::from_millis(10)).await;
    }
}