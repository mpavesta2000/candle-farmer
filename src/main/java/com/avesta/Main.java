package com.avesta;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {

        BinanceChart binanceChart = new BinanceChart();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("Fetching BTCUSDT candles...");
            binanceChart.btcUsdt();
        }, 0, 5, TimeUnit.HOURS);
    }
}
