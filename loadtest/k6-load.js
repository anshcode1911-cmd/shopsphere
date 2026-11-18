// k6 load test — sustained traffic against the public gateway to drive the HPAs.
//   k6 run -e BASE_URL=http://<gateway-external-ip> loadtest/k6-load.js
//   (or) docker run --rm -i grafana/k6 run -e BASE_URL=http://<ip> - < loadtest/k6-load.js
// Watch scaling meanwhile:  kubectl get hpa -n ecommerce -w
import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8080";
const PRODUCTS = ["p-001", "p-002", "p-003", "p-004", "p-005", "p-006", "p-007", "p-008", "p-009", "p-010"];
const ordersCompleted = new Counter("orders_completed");
const ordersFailed = new Counter("orders_not_completed");
const checkoutLatency = new Trend("checkout_latency", true);

export const options = {
  scenarios: {
    shoppers: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "1m", target: 50 },    // warm-up
        { duration: "2m", target: 200 },   // ramp
        { duration: "6m", target: 200 },   // sustained load -> HPA scales out
        { duration: "1m", target: 0 },     // ramp-down -> HPA scales in (after stabilisation)
      ],
      gracefulRampDown: "30s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.05"],                 // < 5 % errors
    "http_req_duration{kind:browse}": ["p(95)<800"],
    "http_req_duration{kind:checkout}": ["p(95)<3000"],
  },
};

const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];

export default function () {
  const user = `u-${String(1 + Math.floor(Math.random() * 200)).padStart(4, "0")}`;
  const headers = { "Content-Type": "application/json", "X-User-Id": user };
  const r = Math.random();

  if (r < 0.55) {
    const res = http.get(`${BASE}/api/products`, { headers, tags: { kind: "browse" } });
    check(res, { "list 200": (x) => x.status === 200 });
  } else if (r < 0.80) {
    const res = http.get(`${BASE}/api/products/${pick(PRODUCTS)}`, { headers, tags: { kind: "browse" } });
    check(res, { "detail 200": (x) => x.status === 200 });
  } else if (r < 0.92) {
    const res = http.post(`${BASE}/api/cart`, JSON.stringify({ product_id: pick(PRODUCTS), quantity: 1 }),
      { headers, tags: { kind: "browse" } });
    check(res, { "cart 200": (x) => x.status === 200 });
  } else {
    const body = JSON.stringify({
      user_id: user,
      items: [{ product_id: pick(PRODUCTS), quantity: 1 + Math.floor(Math.random() * 2) }],
    });
    const res = http.post(`${BASE}/api/orders`, body, { headers, tags: { kind: "checkout" } });
    checkoutLatency.add(res.timings.duration);
    const ok = check(res, { "order 201": (x) => x.status === 201 });
    if (ok && res.json("status") === "COMPLETED") ordersCompleted.add(1);
    else ordersFailed.add(1);
  }
  sleep(0.2 + Math.random() * 0.6);
}
