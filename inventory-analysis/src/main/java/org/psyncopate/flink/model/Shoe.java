package org.psyncopate.flink.model;

import java.util.Date;

import org.psyncopate.flink.utils.MongoDateDeserializer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Shoe {
    private String id;
    private String brand;
    private String name;
    private int sale_price;
    private double rating;
    private String quantity;

    @JsonDeserialize(using = MongoDateDeserializer.class)
    private Date timestamp;
    
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getBrand() {
        return brand;
    }
    public void setBrand(String brand) {
        this.brand = brand;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public double getSale_price() {
        return sale_price;
    }
    public void setSale_price(int sale_price) {
        this.sale_price = sale_price;
    }
    public double getRating() {
        return rating;
    }
    public void setRating(double rating) {
        this.rating = rating;
    }
    public String getQuantity() {
        return quantity;
    }
    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }
   
    public Date getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
    @Override
    public String toString() {
        return "Shoe [id=" + id + ", brand=" + brand + ", name=" + name + ", sale_price=" + sale_price + ", rating="
                + rating + ", quantity=" + quantity + ", timestamp=" + timestamp + "]";
    }
    
}
