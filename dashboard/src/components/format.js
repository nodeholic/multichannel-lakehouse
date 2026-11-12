// Number formatting shared by every page. Amounts are Vietnamese dong (VND).

const compact = new Intl.NumberFormat("en-US", {notation: "compact", maximumFractionDigits: 2});
const integer = new Intl.NumberFormat("en-US");
const percent = new Intl.NumberFormat("en-US", {style: "percent", maximumFractionDigits: 1});

/** 2657348800 -> "2.66B ₫" */
export const vnd = (value) => `${compact.format(value)} ₫`;

/** 1094 -> "1,094" */
export const count = (value) => integer.format(value);

/** 0.0868 -> "8.7%" */
export const pct = (value) => percent.format(value);

/** Channel colors, fixed so a channel keeps its color on every chart. */
export const channelColor = {
  domain: ["Shopify", "Shopee", "Lazada", "TikTok Shop"],
  range: ["#5e9e6e", "#e4744d", "#4e79c7", "#2b2b2b"],
  legend: true
};
