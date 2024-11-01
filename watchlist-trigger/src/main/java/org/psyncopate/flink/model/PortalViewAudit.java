package org.psyncopate.flink.model;

import java.util.Date;

import org.psyncopate.flink.utils.MongoDateDeserializer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PortalViewAudit extends Event{

    private String product_id;
    private String user_id;
    @JsonDeserialize(using = MongoDateDeserializer.class)
    private Date timestamp;

    

    @Override
    public String getProduct_id() {
        return product_id;
    }
    @Override
    public String getCustomer_id() {
        return user_id;
    }
    @Override
    public Date getTimestamp() {
        return timestamp;
    }
    public void setProduct_id(String productId) {
        this.product_id = productId;
    }
    public void setUser_id(String userId) {
        this.user_id = userId;
    }
    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
    @Override
    public String toString() {
        return "PortalViewAudit [productId=" + product_id + ", userId=" + user_id + ", timestamp=" + timestamp + "]";
    }
    
    
    
}
