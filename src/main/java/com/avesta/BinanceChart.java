package com.avesta;

import com.avesta.Model.Candle;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import kong.unirest.json.JSONArray;
import lombok.AllArgsConstructor;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class BinanceChart {

    private final String dbUrl = "jdbc:mysql://localhost:3306/cryptodataset";
    private final String dbUser = "root";
    private final String dbPassword = "";

    public void btcUsdt() {
        String counter = "1000";
        try {
            HttpResponse<String> response = Unirest.get(
                            "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=6h&limit="+counter)
                    .asString();

            JSONArray candles = new JSONArray(response.getBody());
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

            if (isDBEmpty()) {
                for (int i = 0; i < candleList.size() - 1; i++) {
                    saveCandleToDB(candleList.get(i));
                }
                System.out.println("Initial " + counter + " candles saved to DB.");
            }

            Candle lastCandle = candleList.get(candleList.size() - 1);
            long now = System.currentTimeMillis();

            if (now >= lastCandle.closeTime) {
                if (!isCandleInDB(lastCandle.openTime)) {
                    saveCandleToDB(lastCandle);
                    System.out.println("New candle saved: " + lastCandle);
                } else {
                    System.out.println("Candle already exists in DB.");
                }
            } else {
                System.out.println("Candle is still live, waiting for close.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean isDBEmpty() {
        String query = "SELECT COUNT(*) FROM candles";
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                return rs.getInt(1) == 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
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
