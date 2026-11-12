---
title: Data quality
toc: false
---

```js
import {count, pct} from "./components/format.js";

const pipeline = FileAttachment("data/pipeline.json").json();
```

# Data quality

Real sources send duplicates and broken payloads. The Silver layer checks every event against the rules of a
**valid** event, quarantines the ones that break a rule, and keeps one copy of duplicated webhooks.
The generator injects these problems on purpose.

<div class="grid grid-cols-4">
  <div class="card"><h2>Raw events (Bronze)</h2><span class="big">${count(pipeline.raw_events)}</span></div>
  <div class="card"><h2>Quarantined</h2><span class="big">${count(pipeline.rejected_events)}</span>
    <p class="muted">${pct(pipeline.rejected_events / pipeline.raw_events)} of raw events</p></div>
  <div class="card"><h2>Duplicates removed</h2><span class="big">${count(pipeline.duplicate_events)}</span></div>
  <div class="card"><h2>Orders in Silver</h2><span class="big">${count(pipeline.orders)}</span>
    <p class="muted">${count(pipeline.order_items)} order lines</p></div>
</div>

<div class="grid grid-cols-2">
  <div class="card">

## From raw events to clean events

```js
const funnel = [
  {step: "Raw events", events: pipeline.raw_events},
  {step: "Quarantined", events: pipeline.rejected_events},
  {step: "Duplicates", events: pipeline.duplicate_events},
  {step: "Clean events", events: pipeline.clean_events}
];
```

```js
resize((width) => Plot.plot({
  width,
  height: 220,
  marginLeft: 100,
  x: {grid: true, label: "Events"},
  y: {label: null, domain: funnel.map((d) => d.step)},
  color: {domain: funnel.map((d) => d.step), range: ["#8a8f98", "#d1495b", "#e9a03b", "#5e9e6e"]},
  marks: [
    Plot.barX(funnel, {y: "step", x: "events", fill: "step", tip: true}),
    Plot.text(funnel, {y: "step", x: "events", text: (d) => count(d.events), textAnchor: "start", dx: 4})
  ]
}))
```

  </div>
  <div class="card">

## Why events were quarantined

```js
resize((width) => Plot.plot({
  width,
  height: 220,
  marginLeft: 130,
  x: {grid: true, label: "Events"},
  y: {label: null},
  marks: [
    Plot.barX(pipeline.reject_reasons, {y: "reject_reason", x: "events", fill: "#d1495b", sort: {y: "-x"}, tip: true}),
    Plot.text(pipeline.reject_reasons, {y: "reject_reason", x: "events", text: "events", textAnchor: "start", dx: 4})
  ]
}))
```

  </div>
</div>

| `reject_reason` | A valid event has… |
|---|---|
| `missing_order_id` | a non-empty `order_id` |
| `missing_customer_id` | a `customer.customer_id` |
| `invalid_timestamp` | `created_at` and `updated_at` that parse as timestamps |
| `unknown_status` | a status in PENDING, PROCESSING, COMPLETED, CANCELLED, REFUNDED |
| `unknown_channel` | a channel in SHOPIFY, SHOPEE, TIKTOK_SHOP, LAZADA |
| `empty_items` | at least one item |
| `invalid_item` | items with quantity > 0, price ≥ 0, and 0 ≤ discount ≤ quantity × price |
