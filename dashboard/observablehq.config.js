// Observable Framework configuration for the public dashboard.
// The data files in src/data/ are produced by src/export_dashboard_data.py from the pipeline run.
// Framework emits relative links, so the site works under the GitHub Pages sub-path
// (https://nodeholic.github.io/multichannel-lakehouse/) without extra configuration.
export default {
  title: "Multi-Channel Lakehouse",
  root: "src",
  theme: "dashboard", // light and dark, follows the viewer's system setting
  pages: [
    {name: "Sales overview", path: "/"},
    {name: "Data quality", path: "/data-quality"}
  ],
  head: '<meta name="description" content="Dashboard of a multi-channel e-commerce lakehouse built with DuckDB and dbt.">',
  header:
    '<a href="https://github.com/nodeholic/multichannel-lakehouse" target="_blank" rel="noopener">Source code on GitHub ↗</a>',
  footer:
    "All data is synthetic (Faker, fixed seed). Rebuilt by GitHub Actions from the pipeline on every merge.",
  toc: false,
  search: false
};
