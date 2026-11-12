---
title: Sales overview
toc: false
---

```js
import {vnd, count, pct, channelColor} from "./components/format.js";

const kpis = FileAttachment("data/kpis.json").json();
const revenueByDay = FileAttachment("data/revenue_by_day.json").json();
const channels = FileAttachment("data/channels.json").json();
const products = FileAttachment("data/products.json").json();
const tiers = FileAttachment("data/tiers.json").json();
const build = FileAttachment("data/build.json").json();
```

# Sales overview

Orders from four sales channels, cleaned in the **Silver** layer and modeled as a **Gold** star schema with dbt.
Period: **${kpis.first_day}** to **${kpis.last_day}**. Revenue counts completed orders only.

<div class="grid grid-cols-4">
  <div class="card"><h2>Revenue</h2><span class="big">${vnd(kpis.revenue)}</span></div>
  <div class="card"><h2>Completed orders</h2><span class="big">${count(kpis.completed_orders)}</span>
    <p class="muted">of ${count(kpis.all_orders)} orders placed</p></div>
  <div class="card"><h2>Average order value</h2><span class="big">${vnd(kpis.revenue / kpis.completed_orders)}</span></div>
  <div class="card"><h2>Refund rate</h2><span class="big">${pct(kpis.refund_rate)}</span>
    <p class="muted">refunded / delivered orders</p></div>
</div>

<div class="card">

## Revenue per day and channel

```js
resize((width) => Plot.plot({
  width,
  height: 320,
  x: {type: "band", label: null, tickFormat: (d) => d.slice(5)},
  y: {grid: true, label: "Revenue (₫)", tickFormat: "s"},
  color: channelColor,
  marks: [
    Plot.barY(revenueByDay, {x: "day", y: "revenue", fill: "channel", tip: {format: {y: vnd}}}),
    Plot.ruleY([0])
  ]
}))
```

</div>

<div class="grid grid-cols-2">
  <div class="card">

## Revenue and refund rate per channel

```js
resize((width) => Plot.plot({
  width,
  height: 220,
  marginLeft: 90,
  x: {grid: true, label: "Revenue (₫)", tickFormat: "s"},
  y: {label: null},
  color: channelColor,
  marks: [
    Plot.barX(channels, {y: "channel", x: "revenue", fill: "channel", sort: {y: "-x"}, tip: {format: {x: vnd}}}),
    Plot.text(channels, {y: "channel", x: "revenue", text: (d) => `refunds ${pct(d.refund_rate)}`,
      textAnchor: "end", dx: -6, fill: "white"})
  ]
}))
```

  </div>
  <div class="card">

## Revenue per membership tier

Tier **at the time of purchase**, thanks to the SCD Type 2 customer dimension.

```js
resize((width) => Plot.plot({
  width,
  height: 190,
  marginLeft: 80,
  x: {grid: true, label: "Revenue (₫)", tickFormat: "s"},
  y: {label: null, domain: tiers.map((d) => d.tier)},
  marks: [
    Plot.barX(tiers, {y: "tier", x: "revenue", fill: "var(--theme-foreground-focus)", tip: {format: {x: vnd}}}),
    Plot.text(tiers, {y: "tier", x: "revenue", text: (d) => `${count(d.orders)} orders`, textAnchor: "start", dx: 4})
  ]
}))
```

  </div>
</div>

<div class="grid grid-cols-2">
  <div class="card">

## Top products

```js
resize((width) => Plot.plot({
  width,
  height: 360,
  marginLeft: 150,
  x: {grid: true, label: "Revenue (₫)", tickFormat: "s"},
  y: {label: null},
  color: {legend: true},
  marks: [
    Plot.barX(products, {y: "product", x: "revenue", fill: "category", sort: {y: "-x"},
      tip: {channels: {units: "units"}, format: {x: vnd}}})
  ]
}))
```

  </div>
  <div class="card">

## Revenue per category

```js
resize((width) => Plot.plot({
  width,
  height: 200,
  marginLeft: 90,
  x: {grid: true, label: "Revenue (₫)", tickFormat: "s"},
  y: {label: null},
  color: {legend: false},
  marks: [
    Plot.barX(products, Plot.groupY({x: "sum"}, {y: "category", x: "revenue", fill: "category", sort: {y: "-x"},
      tip: {format: {x: vnd}}}))
  ]
}))
```

  </div>
</div>

<p class="muted">Built from commit <code>${build.commit}</code> at ${build.generated_at}.</p>
