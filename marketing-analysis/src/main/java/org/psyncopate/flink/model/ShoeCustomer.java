package org.psyncopate.flink.model;

import java.util.Date;

import org.psyncopate.flink.utils.MongoDateDeserializer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShoeCustomer {

    public String id;
    public String first_name;
    public String last_name;
    public String email;
    public String phone;
    public String zip_code;
    public String state;
    public String country;
    @JsonDeserialize(using = MongoDateDeserializer.class)
    private Date timestamp;
    public Date getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getFirst_name() {
        return first_name;
    }
    public void setFirst_name(String firstName) {
        this.first_name = firstName;
    }
    public String getLast_name() {
        return last_name;
    }
    public void setLast_name(String lastName) {
        this.last_name = lastName;
    }
    public String getEmail() {
        return email;
    }
    public void setEmail(String email) {
        this.email = email;
    }
    public String getPhone() {
        return phone;
    }
    public void setPhone(String phone) {
        this.phone = phone;
    }
    public String getZip_code() {
        return zip_code;
    }
    public void setZip_code(String zipCode) {
        this.zip_code = zipCode;
    }
    public String getState() {
        return state;
    }
    public void setState(String state) {
        this.state = state;
    }
    public String getCountry() {
        return country;
    }
    public void setCountry(String country) {
        this.country = country;
    }
    @Override
    public String toString() {
        return "ShoeCustomer [id=" + id + ", firstName=" + first_name + ", lastName=" + last_name + ", email=" + email
                + ", phone=" + phone + ", zipCode=" + zip_code + ", state=" + state + ", country=" + country
                + ", timestamp=" + timestamp + "]";
    }
   
    
}
