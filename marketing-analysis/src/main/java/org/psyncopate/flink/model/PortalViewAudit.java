package org.psyncopate.flink.model;

import java.util.Date;

import org.psyncopate.flink.utils.MongoDateDeserializer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PortalViewAudit {

    private String product_id;
    private String user_id;
    private long view_time;
    private String page_url;
    private String ip;
    @JsonDeserialize(using = MongoDateDeserializer.class)
    private Date ts;
    @JsonDeserialize(using = MongoDateDeserializer.class)
    private Date timestamp;
    public PortalViewAudit() {
    }
    public PortalViewAudit(String productId, String userId, long viewTime, String pageUrl, String ip, Date ts,
            Date timestamp) {
        this.product_id = productId;
        this.user_id = userId;
        this.view_time = viewTime;
        this.page_url = pageUrl;
        this.ip = ip;
        this.ts = ts;
        this.timestamp = timestamp;
    }
    public String getProduct_id() {
        return product_id;
    }
    public void setProduct_id(String productId) {
        this.product_id = productId;
    }
    public String getUser_id() {
        return user_id;
    }
    public void setUser_id(String userId) {
        this.user_id = userId;
    }
    public long getView_time() {
        return view_time;
    }
    public void setView_time(long viewTime) {
        this.view_time = viewTime;
    }
    public String getPage_url() {
        return page_url;
    }
    public void setPage_url(String pageUrl) {
        this.page_url = pageUrl;
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
        return "PortalViewAudit [productId=" + product_id + ", userId=" + user_id + ", viewTime=" + view_time
                + ", pageUrl=" + page_url + ", ip=" + ip + ", ts=" + ts + ", timestamp=" + timestamp + "]";
    }
    
}
