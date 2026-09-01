---
name: governance-mapping
description: Governance and access-control migration harness for Databricks migrations. Use when inventorying legacy grants/roles/masking policies, proposing their Unity Catalog equivalents, or verifying access parity before cutover.
---

# Governance Mapping Harness

Migrate the mechanical half of governance (who can see what) and verify parity; the policy half (how the customer's IAM/PII regime should be designed) stays a human decision recorded as D-entries. Three capabilities, run in order across the engagement.

## 1. Inventory (during estate inventory)
Extract, from source metadata, for every in-scope object: grants (grantee, privilege, grantable), roles and role memberships, service accounts that hold access, and column-masking / row-level policies. The attached source-dialect skill's governance-discovery entry names the system tables (e.g. Teradata `DBC.AllRightsV`, Redshift `pg_default_acl`/`HAS_TABLE_PRIVILEGE` sweeps, Oracle `DBA_TAB_PRIVS`/`DBA_ROLES`/redaction policies). Output: `.migration/08_governance_inventory.md`, one row per (object, grantee, privilege/policy), each row FACT with its metadata query cited. Grants are census objects: coverage arithmetic includes them.

## 2. Mapping proposal (attached at STOP C)
For each inventory row, propose the UC equivalent: catalog/schema/table `GRANT`, account group membership, or UC masking function / row filter. Emit two artifacts: a machine-readable mapping (source row -> UC statement -> status PROPOSED/APPROVED/GAP) and an executable GRANT/policy script generated from approved rows only. Rules:
- Map roles to groups, never to individual-user grant sprawl; where the source granted to users directly, propose a group and flag it.
- Any source policy with no UC counterpart is a **GAP row**, never silently dropped; gaps need an explicit user acceptance or a D-entry with an owner.
- The script targets the migration catalog during the engagement; production-catalog grants are generated but executed only as a STOP E action.

## 3. Parity verification (cutover entry criterion)
Diff effective access legacy vs UC: for every (principal, object) pair, resolve what each side actually permits (role/group expansion included) and report three sets: matched, lost-access, gained-access. Lost or gained entries block cutover eligibility until user-accepted with a reason. Same for policies: every source masking/row policy has a verified UC counterpart or a documented accepted gap. The parity report goes in the evidence pack.

## Rules
- Fan-out children never create, modify, or execute grants; the approved script runs once, in the parent, under the migration principal.
- Never invent a privilege mapping when semantics differ (e.g. source column-level SELECT vs UC table grant plus mask): flag as GAP for human decision.
- Secrets and service-account credentials are referenced by name only; the inventory records that an account exists, never its credential.

## Known traps (append per engagement)
- PUBLIC grants in the source are easy to overlook and dangerous to replicate literally; always surface as a flagged proposal, never auto-map.
- Ownership semantics differ: source object owners often carry implicit rights UC does not confer; enumerate implicit rights explicitly.
- Nested role hierarchies can exceed UC group-nesting expectations; flatten with evidence, do not approximate.
