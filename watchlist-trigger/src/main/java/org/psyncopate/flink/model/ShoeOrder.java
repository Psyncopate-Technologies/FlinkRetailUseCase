package org.psyncopate.flink.model;

import java.util.Date;

import org.psyncopate.flink.utils.MongoDateDeserializer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ShoeOrder extends Event{
    private String customer_id;
    private String product_id;
    @JsonDeserialize(using = MongoDateDeserializer.class)
    private Date timestamp;


    @Override
    public String getProduct_id() {
        return product_id;
    }
    @Override
    public String getCustomer_id() {
        return customer_id;
    }
    @Override
    public Date getTimestamp() {
        return timestamp;
    }
    public void setCustomer_id(String userId) {
        this.customer_id = userId;
    }
    public void setProduct_id(String productId) {
        this.product_id = productId;
    }
    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
    @Override
    public String toString() {
        return "ShoeOrder [userId=" + customer_id + ", productId=" + product_id + ", timestamp=" + timestamp + "]";
    }
    
    
   
    
    
}
