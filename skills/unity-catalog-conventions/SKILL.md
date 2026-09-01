---
name: unity-catalog-conventions
description: Unity Catalog layout, naming, and governance patterns for migrated estates. Use when creating catalogs/schemas/tables for a migration, mapping legacy namespaces to UC, or implementing grants, row filters, and column masks.
---

# Unity Catalog Conventions

The concrete names always come from the engagement's target-state artifact; this skill carries the patterns to propose and the traps.

## Layout patterns
- Default mapping: legacy database/schema -> UC `catalog.schema`, one catalog per environment (`<name>_dev`, `<name>_prod`) or per domain, per the customer's stated convention.
- Migration work lands in a dedicated migration catalog, isolated from production catalogs, owned by the migration principal. Per-unit isolated areas during fan-out: `<catalog>.<schema>__wave<N>_<unit>` or a batch-scoped schema, dropped and recreated idempotently.
- Medallion (bronze/silver/gold) applies to re-architected pipelines; a like-for-like migration keeps the legacy shape first and defers medallion refactors to a named follow-up, unless the target state says otherwise.

## Naming
- Lowercase snake_case for catalogs, schemas, tables, columns. Preserve legacy table/column names in like-for-like migrations even when ugly; renames break consumers and recon.
- Record any forced rename (reserved words, illegal characters) in the unit's mapping table so recon joins on the mapping, not on name equality.

## Governance
- Grants follow least privilege: migration principal owns the migration catalog; consumers get SELECT on gold/published schemas only at cutover.
- Legacy row-level security, masking, and retention rules are D8 dependencies: capture the legacy contract, implement as UC row filters and column masks, and include a masked-vs-unmasked recon check.

## Traps
- Managed vs external tables: decide per target state before backfill; converting later moves data.
- Case sensitivity and collation differ from legacy engines; string-keyed joins in recon need explicit normalization rules from the tolerance record.
