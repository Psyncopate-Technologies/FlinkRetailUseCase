package org.psyncopate.flink.model;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;




@JsonPropertyOrder({"claim_id", "member_id", "diagnosis_code", "diagnosis_description", "diagnosis_date", "lab_results", "event_time", "additionalField"})
public class ClaimDiagnosis {
    private String claim_id;
    private String member_id;
    private String diagnosis_code;
    private String diagnosis_description;
    private String diagnosis_date;
    private String lab_results;
    private String event_time;
    private String additionalField;
    //private String addnlfield;
    public String getClaim_id() {
        return claim_id;
    }
    public void setClaim_id(String claim_id) {
        this.claim_id = claim_id;
    }
    public String getMember_id() {
        return member_id;
    }
    public void setMember_id(String member_id) {
        this.member_id = member_id;
    }
    public String getDiagnosis_code() {
        return diagnosis_code;
    }
    public void setDiagnosis_code(String diagnosis_code) {
        this.diagnosis_code = diagnosis_code;
    }
    public String getDiagnosis_description() {
        return diagnosis_description;
    }
    public void setDiagnosis_description(String diagnosis_description) {
        this.diagnosis_description = diagnosis_description;
    }
    public String getDiagnosis_date() {
        return diagnosis_date;
    }
    public void setDiagnosis_date(String diagnosis_date) {
        this.diagnosis_date = diagnosis_date;
    }
    public String getLab_results() {
        return lab_results;
    }
    public void setLab_results(String lab_results) {
        this.lab_results = lab_results;
    }
    public String getEvent_time() {
        return event_time;
    }
    public void setEvent_time(String event_time) {
        this.event_time = event_time;
    }
    public String getAdditionalField() {
        return additionalField;
    }
    public void setAdditionalField(String additionalField) {
        this.additionalField = additionalField;
    }

    // Custom getter methods to convert String to Timestamp
    public Timestamp getDiagnosisDateAsTimestamp() {
        return Timestamp.valueOf(diagnosis_date);
    }

    public Timestamp getEventTimeAsTimestamp() {
        return Timestamp.valueOf(event_time);
    }

    @Override
    public String toString() {
        return "ClaimDiagnosis [claim_id=" + claim_id + ", member_id=" + member_id + ", diagnosis_code="
                + diagnosis_code + ", diagnosis_description=" + diagnosis_description + ", diagnosis_date="
                + diagnosis_date + ", lab_results=" + lab_results + ", event_time=" + event_time + ", additionalField="
                + additionalField + "]";
    }

   
}
