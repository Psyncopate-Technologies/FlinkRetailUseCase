package org.psyncopate.flink.model;

import java.text.DecimalFormat;

public class TotalSaleRecord {

    public TotalSaleRecord(double avgPrice, String windowStart, String windowEnd) {
        this.totalPrice = avgPrice;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }
    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("#.##");
        return "AverageSaleRecord [avgPrice=" + df.format(totalPrice) + ", windowStart=" + windowStart + ", windowEnd=" + windowEnd
                + "]";
    }
    private double totalPrice;
    private String windowStart;
    private String windowEnd;
    public TotalSaleRecord() {
    }
    public double getTotalPrice() {
        return totalPrice;
    }
    public void setTotalPrice(double avgPrice) {
        this.totalPrice = avgPrice;
    }
    public String getWindowStart() {
        return windowStart;
    }
    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }
    public String getWindowEnd() {
        return windowEnd;
    }
    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }
    
}
