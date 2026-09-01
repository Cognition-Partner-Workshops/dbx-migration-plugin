# DBX Migration Intake (pre-kickoff)

Fill this in before the first session starts. Anything left blank will be discovered by probing where possible, or proposed as a default you confirm once at STOP A. The goal: kickoff confirms this document, it does not interview you.

Every field is one of: **FACT** (you filled it), **DISCOVERED** (Devin probed it), or **PROPOSED** (Devin defaulted it, you confirm at STOP A).

## 1. Source estate
| Field | Value | Notes |
|---|---|---|
| Source system + version | | e.g. Redshift ra3, Teradata 17.20, Informatica PC 10.5, Oracle 19c |
| Estate headline size | | rough object counts: tables/views/procs/mappings/jobs |
| What loads it / what reads it | | ingestion tools, BI tools, downstream consumers, at headline level |
| Query history available? | | yes/no; this is the consumer-detection evidence source |

## 2. Target
| Field | Value | Notes |
|---|---|---|
| Databricks workspace URL(s) | | |
| Target catalog / schema layout | | blank = medallion default proposed |
| Warehouse / compute to use | | blank = discovered from workspace |
| Repo(s) for migrated code + docs | | |

## 3. Access (fire these before kickoff; credential lead time is usually the serial floor)
| Field | Value | Notes |
|---|---|---|
| Legacy read-only credential (secret name) | | assessment tier: metadata + read-only |
| Databricks credential (secret name) | | migration tier: write only to migration catalog |
| Federation / JDBC path approvable? | | yes/no/needs-review; highest-leverage early request |
| Security reviewer contact | | receives the three-tier access model doc |

## 4. Correctness contract
| Field | Value | Notes |
|---|---|---|
| Recon mode | | LIVE (federation/dual-run) or DEGRADED (snapshots); blank = LIVE proposed |
| Numeric tolerances | | blank = exact-match proposed, deviations surfaced per type |
| Row-diff size threshold | | blank = default proposed; above it: keyed sampling + full aggregates |
| Legacy query concurrency cap | | max parallel recon queries against the legacy system |

## 5. Process
| Field | Value | Notes |
|---|---|---|
| Stop routing | | Slack DM / Slack channel / Teams webhook secret name / web session only; Slack is the two-way path (approve by replying in-thread), Teams webhooks announce only |
| Daily digest | | Optional: one summary message at an agreed hour (wave-close headline, status delta, what awaits you); off by default |
| Question style | | default: one at a time, with options |
| PR reviewer(s) + turnaround SLA | | reviewer throughput is a real term in the wall-clock math |
| Fan-out width preference | | blank = default 20 proposed, pilot at <= 5 |
| Data-load posture | | federation-first / backfill-first / CDC-coexistence; blank = proposed per family |
| Cutover principal holder | | the human who authorizes and executes STOP E |

---
How this is consumed: attach this file to the front-door session (or commit it to the docs repo). The front door reads it, probes the environment to fill DISCOVERED fields, proposes defaults for the rest, and presents the completed set once at STOP A for a single confirmation.
