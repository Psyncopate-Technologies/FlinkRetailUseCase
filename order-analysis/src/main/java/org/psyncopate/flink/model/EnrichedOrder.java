package org.psyncopate.flink.model;

public class EnrichedOrder {
    private ShoeOrder order;
    private Shoe shoe;
    public EnrichedOrder(ShoeOrder order, Shoe shoe) {
        this.order = order;
        this.shoe = shoe;
    }
    public ShoeOrder getOrder() {
        return order;
    }
    public void setOrder(ShoeOrder order) {
        this.order = order;
    }
    public Shoe getShoe() {
        return shoe;
    }
    public void setShoe(Shoe shoe) {
        this.shoe = shoe;
    }
    @Override
    public String toString() {
        return "EnrichedOrder [order=" + order + ", shoe=" + shoe + "]";
    }
}
