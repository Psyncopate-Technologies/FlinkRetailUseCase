package org.psyncopate.flink.model;

import java.util.Date;

public abstract class Event {
    public abstract String getProduct_id();
    public abstract String getCustomer_id();
    public abstract Date getTimestamp();
    
}
