package com.shopsphere.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Attaches window boundaries + key to the aggregate and renders the JSON result record. */
public class WindowResultFunction
        extends ProcessWindowFunction<WindowAggregator.Acc, String, String, TimeWindow> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WindowResultFunction.class);
    private transient ObjectMapper mapper;

    @Override
    public void process(String eventType, Context ctx, Iterable<WindowAggregator.Acc> aggs, Collector<String> out)
            throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        WindowAggregator.Acc acc = aggs.iterator().next();
        String json = toJson(mapper, eventType, ctx.window().getStart(), ctx.window().getEnd(), acc);
        LOG.info("window result {}", json); // -> YARN container log -> Cloud Logging
        out.collect(json);
    }

    static String toJson(ObjectMapper mapper, String eventType, long start, long end, WindowAggregator.Acc acc)
            throws Exception {
        ObjectNode n = mapper.createObjectNode();
        n.put("windowStart", start);
        n.put("windowEnd", end);
        n.put("eventType", eventType);
        n.put("eventCount", acc.count);
        n.put("uniqueUsers", acc.users.size());
        n.put("revenue", Math.round(acc.revenue * 100.0) / 100.0);
        n.put("computedAt", System.currentTimeMillis());
        return mapper.writeValueAsString(n);
    }
}
