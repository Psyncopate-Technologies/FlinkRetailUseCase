package org.psyncopate.flink.model;

public class ProductViewCount {
    private String productId;
    private long viewCount;
    private String starttime;
    private String endtime;

    public ProductViewCount(String productId, long viewCount, String starttime, String endtime) {
        this.productId = productId;
        this.viewCount = viewCount;
        this.starttime = starttime;
        this.endtime = endtime;
    }

    public ProductViewCount() {}

    // Getters and setters
    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }

    

    public String getStarttime() {
        return starttime;
    }

    public void setStarttime(String starttime) {
        this.starttime = starttime;
    }

    public String getEndtime() {
        return endtime;
    }

    public void setEndtime(String endtime) {
        this.endtime = endtime;
    }

    @Override
    public String toString() {
        return "ProductViewCount [productId=" + productId + ", viewCount=" + viewCount + ", starttime=" + starttime
                + ", endtime=" + endtime + "]";
    }
}