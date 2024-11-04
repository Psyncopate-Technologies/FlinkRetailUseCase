package org.psyncopate.flink.model;

import java.util.Date;

import org.psyncopate.flink.utils.MongoDateDeserializer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PortalViewAudit {

    private String productId;
    private String userId;
    private long viewTime;
    private String pageUrl;
    private String ip;
    private Date ts;
    @JsonDeserialize(using = MongoDateDeserializer.class)
    private Date timestamp;
    public PortalViewAudit() {
    }
    public PortalViewAudit(String productId, String userId, long viewTime, String pageUrl, String ip, Date ts,
            Date timestamp) {
        this.productId = productId;
        this.userId = userId;
        this.viewTime = viewTime;
        this.pageUrl = pageUrl;
        this.ip = ip;
        this.ts = ts;
        this.timestamp = timestamp;
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
    public long getViewTime() {
        return viewTime;
    }
    public void setViewTime(long viewTime) {
        this.viewTime = viewTime;
    }
    public String getPageUrl() {
        return pageUrl;
    }
    public void setPageUrl(String pageUrl) {
        this.pageUrl = pageUrl;
    }
    public String getIp() {
        return ip;
    }
    public void setIp(String ip) {
        this.ip = ip;
    }
    public Date getTs() {
        return ts;
    }
    public void setTs(Date ts) {
        this.ts = ts;
    }
    public Date getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
    @Override
    public String toString() {
        return "PortalViewAudit [productId=" + productId + ", userId=" + userId + ", viewTime=" + viewTime
                + ", pageUrl=" + pageUrl + ", ip=" + ip + ", ts=" + ts + ", timestamp=" + timestamp + "]";
    }
    
}
