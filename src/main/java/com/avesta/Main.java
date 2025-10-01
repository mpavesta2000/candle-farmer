package com.avesta;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        // ONE-TIME: Fetch historical data
        HistoricalDataFetcher fetcher = new HistoricalDataFetcher();
        fetcher.fetchHistoricalData(4); // Get last 4 years

        // THEN: Start your live data collection
        BinanceChart binanceChart = new BinanceChart();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("Fetching latest BTCUSDT candle...");
            binanceChart.btcUsdt();
        }, 0, 1, TimeUnit.HOURS);
    }
}