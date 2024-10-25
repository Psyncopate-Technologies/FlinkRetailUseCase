package org.psyncopate.flink.model;

public class CustomerCount {
    private String state;
    private String customerId;
    private String customerName;
    private int customerCount;
    public CustomerCount(String state, String customerId, String customerName, int customerCount) {
        this.state = state;
        this.customerId = customerId;
        this.customerName = customerName;
        this.customerCount = customerCount;
    }
    public String getState() {
        return state;
    }
    public void setState(String state) {
        this.state = state;
    }
    public String getCustomerId() {
        return customerId;
    }
    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }
    public String getCustomerName() {
        return customerName;
    }
    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }
    public int getCustomerCount() {
        return customerCount;
    }
    public void setCustomerCount(int customerCount) {
        this.customerCount = customerCount;
    }
    @Override
    public String toString() {
        return "CustomerCount [state=" + state + ", customerId=" + customerId + ", customerName=" + customerName
                + ", customerCount=" + customerCount + "]";
    }

    
}
