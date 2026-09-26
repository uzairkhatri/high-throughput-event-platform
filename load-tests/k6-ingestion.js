import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  scenarios: {
    ingestion_smoke: {
      executor: "constant-vus",
      vus: 10,
      duration: "30s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
  },
};

export default function () {
  const id = `${__VU}-${__ITER}`;
  const response = http.post(
    "http://localhost:8000/v1/events",
    JSON.stringify({
      tenant_id: "tenant-a",
      event_type: "order.created",
      aggregate_id: `order-${id}`,
      idempotency_key: `load-test-${id}`,
      payload: { amount: 42 },
    }),
    { headers: { "Content-Type": "application/json", "X-Trace-Id": `k6-${id}` } },
  );

  check(response, {
    "accepted or duplicate": (r) => r.status === 202,
  });
  sleep(0.1);
}

