package com.shopsphere.analytics;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental, stateful window aggregation. The accumulator lives in Flink keyed window
 * state (checkpointed to GCS) and holds: event count, the set of distinct users, revenue.
 */
public class WindowAggregator implements AggregateFunction<ShopEvent, WindowAggregator.Acc, WindowAggregator.Acc> {
    private static final long serialVersionUID = 1L;

    public static class Acc implements Serializable {
        private static final long serialVersionUID = 1L;
        public long count;
        public Set<String> users = new HashSet<>();
        public double revenue;
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    @Override
    public Acc add(ShopEvent e, Acc acc) {
        acc.count++;
        if (e.userId != null && !"anonymous".equals(e.userId)) {
            acc.users.add(e.userId);
        }
        if ("order_completed".equals(e.type)) {
            acc.revenue += e.amount;
        }
        return acc;
    }

    @Override
    public Acc getResult(Acc acc) {
        return acc;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        a.count += b.count;
        a.users.addAll(b.users);
        a.revenue += b.revenue;
        return a;
    }
}
