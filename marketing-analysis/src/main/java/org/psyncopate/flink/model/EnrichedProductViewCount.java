package org.psyncopate.flink.model;

public class EnrichedProductViewCount {
    private String productId;
    private String productName;
    private String brand;
    private long viewCount;
    private String starttime;
    private String endtime;

  

    public EnrichedProductViewCount(String productId, String productName, String brand, long viewCount,
            String starttime, String endtime) {
        this.productId = productId;
        this.productName = productName;
        this.brand = brand;
        this.viewCount = viewCount;
        this.starttime = starttime;
        this.endtime = endtime;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public String getBrand() {
        return brand;
    }

    public long getViewCount() {
        return viewCount;
    }

    

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public void setBrand(String brand) {
        this.brand = brand;
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
        return "EnrichedProductViewCount [productId=" + productId + ", productName=" + productName + ", brand=" + brand
                + ", viewCount=" + viewCount + ", starttime=" + starttime + ", endtime=" + endtime + "]";
    }

    // Getters and Setters
}
