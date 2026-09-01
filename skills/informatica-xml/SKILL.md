---
name: informatica-xml
description: Source-dialect skill for Informatica PowerCenter/IICS estates. Use when parsing Informatica XML exports, extracting mapping lineage, or converting mappings/sessions/workflows to Databricks (PySpark/DLT/Jobs). v0 stub, hardened on first engagement.
---

# Informatica PowerCenter / IICS Dialect (v0)

Five sections per the kit's dialect-skill contract. This is the v0 scaffold; the first engagement's pilot wave hardens Enumeration and Lineage (budget extractor engineering there) and every wave appends Known Traps.

## 1. Enumeration
Units are mappings, grouped by workflow. Parse the repository XML export: `FOLDER/MAPPING`, `SESSION` (runtime config), `WORKFLOW` (orchestration). Census key: folder/mapping name. Sessions and workflows attach to their mapping's unit; reusable transformations and mapplets are shared objects (wave 0).

## 2. Lineage
Reads: `SOURCE` definitions plus Source Qualifier SQL overrides (parse the override SQL, not just the source object). Writes: `TARGET` definitions plus update-strategy semantics. Mark INFERRED: any table name built from parameter files (`$$` variables), mapping parameters resolved at runtime, or dynamic SQL in stored-procedure transformations. Parameter files must be collected with the export; without them, lineage over parameterized objects is INFERRED at best.

## 3. Type map
Informatica transformation datatypes -> Delta: decimal(p,s) preserved exactly (precision loss is a recon failure), string widths dropped (Delta strings are unbounded; flag consumers relying on truncation), date/time to timestamp with the engagement timezone rule. Loss-prone rows: high-precision decimals, packed/COMP fields from mainframe sources, string trailing-space semantics.

## 4. Conversion rules
Default target form comes from the engagement target state (PySpark or DLT). Transformation mapping to build out per engagement: Source Qualifier -> read + filter/join pushup; Expression -> withColumn chains; Aggregator -> groupBy (watch sorted-input assumptions); Lookup -> broadcast join (connected) / scalar subquery (unconnected); Update Strategy -> MERGE; Router/Filter -> when/filter; Sequence Generator -> identity column or monotonically_increasing_id with the gap-semantics difference recorded; Normalizer, Java/SQL transformations -> flag as high-risk, hand-convert.

## 5. Known traps (append per engagement)
- Session-level SQL overrides silently replace mapping logic; always diff session config against the mapping before converting.
- Informatica implicit type conversion is looser than Spark; explicit casts at every port boundary.
- High-precision mode changes decimal arithmetic; check the session property before trusting numeric recon.

## 6. Governance discovery
Informatica's own repository ACLs (folder/object permissions, from repository metadata or the admin console export) govern who edits mappings, and rarely need UC equivalents; record them as inventory context. The governance surface that must migrate lives in the **connections**: each relational/app connection names a service account whose database-side grants are the real access to inventory via that database's dialect skill. Watch: shared connection objects reused across folders (one over-privileged service account serving many workflows is the common finding), and OS-profile/parameter-file paths that carry credentials outside the secret manager.
