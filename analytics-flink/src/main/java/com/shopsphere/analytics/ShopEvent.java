package com.shopsphere.analytics;

import java.io.Serializable;

/** Click-stream / order event produced by gateway and order-service into Kafka `shop-events`. */
public class ShopEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public String type;
    public String userId;
    public double amount;
    public long ts;

    public ShopEvent() {}

    public ShopEvent(String type, String userId, double amount, long ts) {
        this.type = type;
        this.userId = userId;
        this.amount = amount;
        this.ts = ts;
    }
}
