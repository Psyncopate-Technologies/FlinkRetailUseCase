package org.psyncopate.flink.model;

public class ProductOrderCount {

    private String productId;
    private long orderCount;
    private String brand;
    private String productName;
    private String windowStart;
    private String windowEnd;
    
    public ProductOrderCount(String productId, long orderCount, String brand, String productName, String windowStart,
            String windowEnd) {
        this.productId = productId;
        this.orderCount = orderCount;
        this.brand = brand;
        this.productName = productName;
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
    
    public String getBrand() {
        return brand;
    }
    public void setBrand(String brand) {
        this.brand = brand;
    }
    public String getProductName() {
        return productName;
    }
    public void setProductName(String productName) {
        this.productName = productName;
    }
    @Override
    public String toString() {
        return "ProductOrderCount [productId=" + productId + ", orderCount=" + orderCount + ", brand=" + brand
                + ", productName=" + productName + ", windowStart=" + windowStart + ", windowEnd=" + windowEnd + "]";
    }
    
}
