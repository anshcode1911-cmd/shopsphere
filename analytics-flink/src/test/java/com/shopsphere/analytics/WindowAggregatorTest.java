package com.shopsphere.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class WindowAggregatorTest {

    @Test
    void countsEventsUniqueUsersAndRevenue() throws Exception {
        WindowAggregator agg = new WindowAggregator();
        WindowAggregator.Acc acc = agg.createAccumulator();
        acc = agg.add(new ShopEvent("order_completed", "u-1", 100.0, 1L), acc);
        acc = agg.add(new ShopEvent("order_completed", "u-1", 50.5, 2L), acc);
        acc = agg.add(new ShopEvent("order_completed", "u-2", 10.0, 3L), acc);
        acc = agg.add(new ShopEvent("order_completed", "anonymous", 0.0, 4L), acc);

        assertEquals(4, acc.count);
        assertEquals(2, acc.users.size());
        assertEquals(160.5, acc.revenue, 1e-9);

        WindowAggregator.Acc other = agg.createAccumulator();
        other = agg.add(new ShopEvent("order_completed", "u-3", 1.0, 5L), other);
        WindowAggregator.Acc merged = agg.merge(acc, other);
        assertEquals(5, merged.count);
        assertEquals(3, merged.users.size());

        JsonNode json = new ObjectMapper().readTree(
                WindowResultFunction.toJson(new ObjectMapper(), "order_completed", 0L, 60_000L, merged));
        assertEquals(3, json.get("uniqueUsers").asInt());
        assertEquals(60_000L, json.get("windowEnd").asLong());
    }

    @Test
    void revenueOnlyFromCompletedOrders() {
        WindowAggregator agg = new WindowAggregator();
        WindowAggregator.Acc acc = agg.add(new ShopEvent("product_view", "u-1", 999.0, 1L), agg.createAccumulator());
        assertEquals(0.0, acc.revenue, 1e-9);
    }

    @Test
    void deserializerSkipsGarbage() throws Exception {
        ShopEventDeserializer d = new ShopEventDeserializer();
        assertNull(d.deserialize("not json".getBytes(StandardCharsets.UTF_8)));
        ShopEvent e = d.deserialize(
                "{\"type\":\"product_view\",\"user_id\":\"u-7\",\"amount\":5,\"ts\":123}".getBytes(StandardCharsets.UTF_8));
        assertTrue(e != null && e.userId.equals("u-7") && e.ts == 123L);
    }
}
