package org.psyncopate.flink.model;

public class ProductOrderCount {

    private String productId;
    private long orderCount;
    private String windowStart;
    private String windowEnd;
    public ProductOrderCount(String productId, long orderCount, String windowStart, String windowEnd) {
        this.productId = productId;
        this.orderCount = orderCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }
    public String getProductId() {
        return productId;
    }
    public void setProductId(String productId) {
        this.productId = productId;
    }
    public long getOrderCount() {
        return orderCount;
    }
    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
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
    @Override
    public String toString() {
        return "ProductOrderCount [productId=" + productId + ", orderCount=" + orderCount + ", windowStart="
                + windowStart + ", windowEnd=" + windowEnd + "]";
    }
    
}
