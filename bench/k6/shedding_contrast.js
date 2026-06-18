import http from "k6/http";
import { check } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const committed = new Counter("committed_writes");
const shed      = new Counter("shed_writes");
const writeLat  = new Trend("write_latency_ms", true);

const BASE_URL   = __ENV.BASE_URL   || "http://localhost:7070";
const OVERLOAD_RPS = parseInt(__ENV.OVERLOAD_RPS || "3000");

export const options = {
  scenarios: {
    overload: {
      executor: "constant-arrival-rate",
      rate: OVERLOAD_RPS,
      timeUnit: "1s",
      duration: "90s",
      preAllocatedVUs: 200,
      maxVUs: 100000,
    },
  },
  summaryTrendStats: ["med", "p(90)", "p(95)", "p(99)", "p(99.9)", "max"],
};

export default function () {
  const id  = `${__VU}-${__ITER}`;
  const key = `key-${(__VU % 50)}`;

  const payload = JSON.stringify({ id, key, value: { ts: Date.now() } });
  const params  = { headers: { "Content-Type": "application/json" } };

  const res = http.post(`${BASE_URL}/events`, payload, params);

  writeLat.add(res.timings.duration);

  if (res.status === 201 || res.status === 200) {
    committed.add(1);
  } else if (res.status === 429) {
    shed.add(1);
  }

  check(res, {
    "not 5xx": (r) => r.status < 500,
  });
}
