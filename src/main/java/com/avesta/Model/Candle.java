package com.avesta.Model;


import lombok.*;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Candle {
    public long openTime;
    public double openPrice;
    public double highPrice;
    public double lowPrice;
    public double closePrice;
    public double volume;
    public long closeTime;
}
