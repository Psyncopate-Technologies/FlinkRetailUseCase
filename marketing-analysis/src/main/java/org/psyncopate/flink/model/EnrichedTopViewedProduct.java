package org.psyncopate.flink.model;

public class EnrichedTopViewedProduct {

    private String productId;
    private String productName;
    private String brand;
    private String userId;
    private String userName;
    private String state;
    private String country;
    private long viewCount;
    private String windowStart;
    private String windowEnd;
    public EnrichedTopViewedProduct(String productId, String productName, String brand, String userId, String userName,
            String state, String country, long viewCount, String windowStart, String windowEnd) {
        this.productId = productId;
        this.productName = productName;
        this.brand = brand;
        this.userId = userId;
        this.userName = userName;
        this.state = state;
        this.country = country;
        this.viewCount = viewCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }
    public String getProductId() {
        return productId;
    }
    public void setProductId(String productId) {
        this.productId = productId;
    }
    public String getProductName() {
        return productName;
    }
    public void setProductName(String productName) {
        this.productName = productName;
    }
    public String getBrand() {
        return brand;
    }
    public void setBrand(String brand) {
        this.brand = brand;
    }
    public String getUserId() {
        return userId;
    }
    public void setUserId(String userId) {
        this.userId = userId;
    }
    public String getUserName() {
        return userName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }
    public String getState() {
        return state;
    }
    public void setState(String state) {
        this.state = state;
    }
    public String getCountry() {
        return country;
    }
    public void setCountry(String country) {
        this.country = country;
    }
    public long getViewCount() {
        return viewCount;
    }
    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
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
        return "EnrichedTopViewedProduct [productId=" + productId + ", productName=" + productName + ", brand=" + brand
                + ", userId=" + userId + ", userName=" + userName + ", state=" + state + ", country=" + country
                + ", viewCount=" + viewCount + ", windowStart=" + windowStart + ", windowEnd=" + windowEnd + "]";
    }
    
}
