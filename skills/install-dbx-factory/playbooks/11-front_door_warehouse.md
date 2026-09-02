Playbook: Front door for SQL warehouse estates (Redshift, Teradata, BigQuery, Synapse, Oracle EDW). Thin intake that pins the engine, loads the dialect skill, sets warehouse-family defaults, and then runs `!dbx_migrate_pipeline` in the same session. No migration method lives here.

## Overview
The customer says "we're moving off Redshift." This playbook turns that into a configured run of the standard chain: engine and version pinned, catalog metadata access confirmed, dialect skill attached, and the warehouse-family defaults set. Everything after intake is the standard chain, unmodified.

## What's Needed From User
- The engine and version, and read access for Devin to the system catalogs (`information_schema`, `svv_*`, `dbc.*`) and query history. Query history is the census's consumer-detection source; if it is unavailable, D4 sweeps lose their best evidence and the user should know.
- Whether a JDBC path from Databricks to the warehouse can be approved (Lakehouse Federation is the default coexistence and recon mechanism for this family; if refused, snapshots become the fallback and recon scope narrows accordingly).
- The ingestion/BI landscape at a headline level (what loads the warehouse, what reads it), to seed the D3/D4 sweeps.

## Procedure
1. **Consume the intake template first**: if a filled `00_intake_template.md` was attached or committed, load it, mark its rows FACT, probe the environment to fill what it left blank (DISCOVERED), and propose defaults for the rest (PROPOSED). Ask live only what neither the template nor a probe can answer.
2. Pin engine and version; probe catalog access with one live metadata query; register D10s for anything blocked.
3. Attach the matching dialect skill (`redshift-sql`, `teradata-bteq`, `bigquery-sql`, ...). If none exists, generic ANSI translation plus a build-the-skill wave-0 item, stated plainly.
4. Set family defaults for the chain: unit = view / procedure / scheduled query / load script; lineage extraction = catalog metadata + view dependency graphs + query history; SQL profile is the dominant surface; the physical design translation (dist/sort keys, partitioning to liquid clustering) is a named dictionary concern; federation-first coexistence.
5. Record intake facts in `.migration/00_context.md` shape. Then run `!dbx_migrate_pipeline` **in this same session**, right away. Do not open a new session, do not ask the user to run it, do not stop here: the orchestrator reads the intake facts you just wrote and begins ingest and setup as normal. The user's next message from Devin is STOP A.

## Specifications
- Deliverable: configured hand-off: engine pinned, access probed, dialect skill attached, family defaults recorded.
- Validation: `!dbx_migrate_pipeline` was invoked in this session and the orchestrator can start ingest without re-asking anything this intake covered.

## Advice and Pointers
- Warehouse migrations look easier than ETL migrations and hide their difficulty in the same two places every time: engine-specific function semantics (numeric truncation, timestamp behavior) and the untracked consumer population. The dictionary and the D4 sweep are where this family's engagements are won.
- Stored procedures are the schedule risk in this family: procedural dialects (Teradata SPL, Redshift plpgsql) convert slower than views; the inventory's complexity ranking should weight them accordingly.
- Federation approval is the single highest-leverage early request; fire it at intake if the user can approve it directly.

## Forbidden Actions
- Do NOT begin inventory, analysis, or conversion here; hand off to the chain.
- Do NOT assume federation is approvable; probe or ask, and register the answer.
- Do NOT let missing query history pass silently; record the D4-evidence gap at intake.
