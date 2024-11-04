package org.psyncopate.flink.model;

import java.util.Date;

import org.psyncopate.flink.utils.MongoDateDeserializer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ShoeOrder {
    private int order_id;
    private String product_id;
    private String customer_id;
    private Date ts;
    @JsonDeserialize(using = MongoDateDeserializer.class)
    private Date timestamp;
    
    public int getOrder_id() {
        return order_id;
    }
    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }
    public String getProduct_id() {
        return product_id;
    }
    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }
    public String getCustomer_id() {
        return customer_id;
    }
    public void setCustomer_id(String customer_id) {
        this.customer_id = customer_id;
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
        return "ShoeOrder [order_id=" + order_id + ", product_id=" + product_id + ", customer_id=" + customer_id
                + ", ts=" + ts + ", timestamp=" + timestamp + "]";
    }
    
    
    
}
