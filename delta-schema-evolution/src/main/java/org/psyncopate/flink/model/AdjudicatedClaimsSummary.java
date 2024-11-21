package org.psyncopate.flink.model;

import java.math.BigDecimal;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;



@JsonPropertyOrder({"claim_id", "total_diagnoses", "total_procedures", "total_cost"})
public class AdjudicatedClaimsSummary {
    private String claim_id;
    private Long total_diagnoses;
    private Long total_procedures;
    private BigDecimal total_cost;
    public String getClaim_id() {
        return claim_id;
    }
    public void setClaim_id(String claim_id) {
        this.claim_id = claim_id;
    }
    public Long getTotal_diagnoses() {
        return total_diagnoses;
    }
    public void setTotal_diagnoses(Long total_diagnoses) {
        this.total_diagnoses = total_diagnoses;
    }
    public Long getTotal_procedures() {
        return total_procedures;
    }
    public void setTotal_procedures(Long total_procedures) {
        this.total_procedures = total_procedures;
    }
    public BigDecimal getTotal_cost() {
        return total_cost;
    }
    public void setTotal_cost(BigDecimal total_cost) {
        this.total_cost = total_cost;
    }
    @Override
    public String toString() {
        return "AdjudicatedClaimsSummary [claim_id=" + claim_id + ", total_diagnoses=" + total_diagnoses
                + ", total_procedures=" + total_procedures + ", total_cost=" + total_cost + "]";
    }

    
    
}
