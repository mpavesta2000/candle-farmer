package com.avesta;

import com.avesta.Model.Candle;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import kong.unirest.json.JSONArray;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class HistoricalDataFetcher {

    private final String dbUrl = "jdbc:mysql://localhost:3306/cryptodataset";
    private final String dbUser = "root";
    private final String dbPassword = "";

    public void fetchHistoricalData(int years) {
        try {
            // Calculate start time (X years ago from now)
            long now = System.currentTimeMillis();
            long startTime = now - ((long) years * 365 * 24 * 60 * 60 * 1000);

            // Get the latest candle in DB (if any)
            long lastCandleTime = getLatestCandleTime();

            if (lastCandleTime > 0) {
                System.out.println("Found existing data. Latest candle: " + new java.util.Date(lastCandleTime));
                // Start from where we left off
                startTime = lastCandleTime + 1;
            }

            int totalCandles = 0;
            long currentStartTime = startTime;

            System.out.println("Starting historical fetch from: " + new java.util.Date(currentStartTime));

            while (currentStartTime < now) {
                System.out.println("Fetching batch starting from: " + new java.util.Date(currentStartTime));

                HttpResponse<String> response = Unirest.get(
                                "https://api.binance.com/api/v3/klines")
                        .queryString("symbol", "BTCUSDT")
                        .queryString("interval", "6h")
                        .queryString("startTime", currentStartTime)
                        .queryString("limit", 1000)
                        .asString();

                JSONArray candles = new JSONArray(response.getBody());

                if (candles.length() == 0) {
                    System.out.println("No more candles to fetch.");
                    break;
                }

                List<Candle> candleList = new ArrayList<>();
                for (int i = 0; i < candles.length(); i++) {
                    JSONArray c = candles.getJSONArray(i);
                    Candle candle = new Candle();
                    candle.openTime = c.getLong(0);
                    candle.openPrice = Double.parseDouble(c.getString(1));
                    candle.highPrice = Double.parseDouble(c.getString(2));
                    candle.lowPrice = Double.parseDouble(c.getString(3));
                    candle.closePrice = Double.parseDouble(c.getString(4));
                    candle.volume = Double.parseDouble(c.getString(5));
                    candle.closeTime = c.getLong(6);
                    candleList.add(candle);
                }

                // Save candles that are fully closed
                int savedCount = 0;
                for (Candle candle : candleList) {
                    // Only save if candle is closed AND not already in DB
                    if (candle.closeTime < now && !isCandleInDB(candle.openTime)) {
                        saveCandleToDB(candle);
                        savedCount++;
                    }
                }

                totalCandles += savedCount;
                System.out.println("Saved " + savedCount + " candles. Total so far: " + totalCandles);

                // Move to next batch: start from the last candle's close time + 1ms
                Candle lastCandle = candleList.get(candleList.size() - 1);
                currentStartTime = lastCandle.closeTime + 1;

                // Small delay to respect API rate limits (not strictly necessary but polite)
                Thread.sleep(100);
            }

            System.out.println("Historical fetch complete. Total candles saved: " + totalCandles);
            System.out.println("Database now contains: " + getTotalCandleCount() + " candles");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private long getLatestCandleTime() {
        String query = "SELECT MAX(close_time) FROM candles";
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private int getTotalCandleCount() {
        String query = "SELECT COUNT(*) FROM candles";
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private boolean isCandleInDB(long openTime) {
        String query = "SELECT COUNT(*) FROM candles WHERE open_time = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setLong(1, openTime);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void saveCandleToDB(Candle candle) {
        String insert = "INSERT INTO candles (open_time, open_price, high_price, low_price, close_price, volume, close_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(insert)) {

            stmt.setLong(1, candle.openTime);
            stmt.setDouble(2, candle.openPrice);
            stmt.setDouble(3, candle.highPrice);
            stmt.setDouble(4, candle.lowPrice);
            stmt.setDouble(5, candle.closePrice);
            stmt.setDouble(6, candle.volume);
            stmt.setLong(7, candle.closeTime);

            stmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}