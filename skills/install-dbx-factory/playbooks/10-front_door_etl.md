Playbook: Front door for ETL-tool estates (Informatica, Talend, Ab Initio, DataStage, SSIS). Thin intake that pins the source family, loads the right dialect skill, sets family-specific defaults, and hands off to `!dbx_migrate_pipeline`. No migration method lives here.

## Overview
The customer says "we're getting off Informatica." This playbook turns that sentence into a correctly-configured run of the standard chain: it identifies the exact tool and version, secures the estate export, attaches the matching source-dialect skill, and pre-answers the intake questions that are the same for every ETL-tool engagement. Everything after intake is the standard chain, unmodified.

## What's Needed From User
- The tool and version (Informatica PowerCenter vs IICS, Talend, Ab Initio, DataStage, SSIS), and how the estate can be exported (XML/repository dump, project export, graph files). If no full export is possible, that is the first D10.
- The scheduler around it (Control-M, Autosys, Airflow, tool-native), because for ETL estates the scheduler is half the migration. **If it is an enterprise scheduler (Control-M, Autosys), flag the engagement as dual-workstream at intake**: scheduler migration is usually owned by a separate team with its own change-control calendar, so name that team's owner now (they attend STOP A) and carry their change-control lead time as a line in the plan's wall-clock math. A single D5 register entry does not contain a Control-M program.
- Rough scale (folders/projects, mapping count) to size the inventory session.

## Procedure
1. **Consume the intake template first**: if a filled `00_intake_template.md` was attached or committed, load it, mark its rows FACT, probe the environment to fill what it left blank (DISCOVERED), and propose defaults for the rest (PROPOSED). Ask live only what neither the template nor a probe can answer; the questions below are the fallback, not the default path.
2. Confirm tool, version, and export mechanism; obtain or request the export (register D10 if blocked).
3. Attach the matching source-dialect skill from the skills catalog (`informatica-xml`, `ab-initio-graphs`, `talend-jobs`, ...). If none exists for this tool, say so: the inventory can still run generically, but conversion quality depends on the skill, so building it becomes wave-0 work, presented as such. Even where a skill exists, **budget extractor hardening as engineering work on the first engagement per dialect**: lineage extraction from parameter indirection and dynamic components is the kit's highest-consequence failure point, and the skill's parser earns trust by surviving one real export, not by existing.
4. Set family defaults for the chain: unit = mapping/job + its session/workflow config; lineage extraction = parser-based from the export (not query history); PIPELINE profile is the dominant workload surface; scheduler edges are first-class D5 entries; parameter-file indirection is a named inventory risk.
5. Record intake facts in `.migration/00_context.md` shape and hand off to `!dbx_migrate_pipeline` (which begins with ingest + setup as normal).

## Specifications
- Deliverable: a configured hand-off to the orchestrator: tool pinned, export secured or requested, dialect skill attached, family defaults recorded.
- Validation: the orchestrator can start ingest without re-asking anything this intake covered.

## Advice and Pointers
- ETL estates are the widest fan-out case in the kit: thousands of mostly-independent mappings. The speed story lives in the parallelism profile the inventory will compute; set expectations at intake that lead times (export access, service principal) gate speed more than conversion does.
- Version matters more than tool: PowerCenter XML and IICS exports parse differently; pin it before promising the parser works.
- Ask for the scheduler export in the same breath as the tool export; it arrives late in most engagements and blocks D5 decisions.

## Forbidden Actions
- Do NOT begin inventory, analysis, or conversion here; hand off to the chain.
- Do NOT proceed on a partial export without registering it and telling the user what coverage arithmetic will and will not prove.
- Do NOT promise conversion of tool features the dialect skill marks unsupported; surface them as named risks at intake.
