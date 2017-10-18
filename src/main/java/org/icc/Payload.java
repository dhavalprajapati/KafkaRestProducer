package org.icc;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

public class Payload implements Serializable{

    private String customerId;
    private String transactionAmnt;
    private String status;

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String clientId) {
        this.customerId = clientId;
    }

    public String getTransactionAmnt() {
        return transactionAmnt;
    }

    public void setTransactionAmnt(String transactionAmnt) {
        this.transactionAmnt = transactionAmnt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Payload(){}

    @Override
    public String toString() {
        return "{\"customerId\":\""+customerId+"\",\"transactionAmnt\":\""+transactionAmnt+"\", \"status\":"+status+"\"}";
    }

}