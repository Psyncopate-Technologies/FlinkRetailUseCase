package org.psyncopate.flink.model;

public class EnrichedOrderNotPlacedAlerts {

    private String productId;
    private String productName;
    private String userId;
    private String userName;
    private String reason;
    private String state;
    private String zip;
    
    public EnrichedOrderNotPlacedAlerts(String productId, String productName, String userId, String userName,
            String reason, String state, String zip) {
        this.productId = productId;
        this.productName = productName;
        this.userId = userId;
        this.userName = userName;
        this.reason = reason;
        this.state = state;
        this.zip = zip;
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
    public String getReason() {
        return reason;
    }
    public void setReason(String reason) {
        this.reason = reason;
    }
    public String getState() {
        return state;
    }
    public void setState(String state) {
        this.state = state;
    }
    public String getZip() {
        return zip;
    }
    public void setZip(String zip) {
        this.zip = zip;
    }
    @Override
    public String toString() {
        return "EnrichedOrderNotPlacedAlerts [productId=" + productId + ", productName=" + productName + ", userId="
                + userId + ", userName=" + userName + ", reason=" + reason + ", state=" + state + ", zip=" + zip + "]";
    }
    
}
