package org.psyncopate.flink.model;

public class TopViewedProduct {
    private String productId;
    private String userId;
    private long viewCount;
    private String windowStart;
    private String windowEnd;
    public TopViewedProduct(String productId, String userId, long viewCount, String windowStart, String windowEnd) {
        this.productId = productId;
        this.userId = userId;
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
    public String getUserId() {
        return userId;
    }
    public void setUserId(String userId) {
        this.userId = userId;
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
        return "TopViewedProduct [productId=" + productId + ", userId=" + userId + ", viewCount=" + viewCount
                + ", windowStart=" + windowStart + ", windowEnd=" + windowEnd + "]";
    }
    
}


