# New Relic Plugin for Claude

New Relic observability intelligence for Claude Code. Investigate APM performance, analyze cloud costs, debug Kubernetes, write NRQL queries, and respond to alerts — all from your terminal using New Relic telemetry data.

## Included Skills

- **`apm`** — Application Performance Monitoring and transaction analysis. Use when investigating application errors, slow response times, throughput issues, or transaction-level problems.
- **`finops`** — Cloud FinOps cost analysis across AWS, Azure, and GCP. Use when investigating cloud spend, cost anomalies, spikes, or budgets. Requires Cloud Cost Intelligence data ingested into New Relic.
- **`kubernetes`** — Kubernetes diagnosis and debugging using New Relic telemetry. Use for pod crashes, CrashLoopBackOff, OOMKills, evictions, scheduling failures, node pressure, and scaling issues. Requires `nri-kubernetes` / kube-state-metrics data.
- **`newrelic-mcp`** — New Relic MCP server workflows and NRQL-driven investigations. Use to query logs, metrics, traces, alerts, incidents, dashboards, or infrastructure.

## MCP Server

The plugin connects to the hosted New Relic MCP server, declared in [.mcp.json](.mcp.json):

```json
{
  "mcpServers": {
    "newrelic": {
      "type": "http",
      "url": "https://mcp.newrelic.com/mcp/"
    }
  }
}
```

The server uses OAuth; you'll be prompted to authenticate to your New Relic account on first use.

## Project Structure

```
.claude-plugin/
  plugin.json          # Plugin metadata
.mcp.json              # New Relic MCP server connection
skills/
  apm/
    SKILL.md
    references/         # ERROR_ANALYSIS.md, PERFORMANCE_METRICS.md
  finops/
    SKILL.md
  kubernetes/
    SKILL.md
  newrelic-mcp/
    skill.md
    references/         # nrql-patterns.md
NOTICE
README.md
```

## Metadata

- **Name:** `newrelic`
- **Version:** `1.0.0`
- **Author:** New Relic
- **License:** `Apache-2.0`
- **Homepage:** https://newrelic.com/
- **Repository:** https://github.com/newrelic/newrelic-claude-plugin

## License

Licensed under the Apache License, Version 2.0. See the [NOTICE](NOTICE) file for details.
