#!/usr/bin/env python3
"""Fixture round-trip for skills/informatica-xml/SKILL.md sections 1-2 against the Albion estate XML/text.

FIXTURE VALIDATION ONLY. This parses the read-only export files (POWERMART XML, .par, ksh wrappers, Control-M
DEFTABLE, crontab) and checks that the enumeration, lineage, override, parameter and scheduler rules written in the
skill produce the census recorded in SKILL.md section 1.4. Nothing here talks to a PowerCenter repository, an
Integration Service, a source database, or Databricks; see the "Not verified live" list in each example NOTE.md.

Usage:  python3 fixture_roundtrip.py /path/to/albion-insurance-data-estate
Exit code 0 when every assertion holds; the report lists each rule and what the XML text yielded.
"""
from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

WORKFLOW_XML = [
    "wf_BILLING_PREMIUM_RECON",
    "wf_CLAIMS_FNOL_INTRADAY",
    "wf_PARTY_MDM_SYNC",
    "wf_POLICY_MASTER_DAILY",
    "wf_REINSURANCE_BORDEREAUX_MONTHLY",
]

# SKILL.md section 1.4 census, restated as expectations (folder, mapping, transformation-type mix, reusable lookups).
EXPECTED_CENSUS = {
    "wf_BILLING_PREMIUM_RECON": ("FIN_BILLING", "m_BILLING_PREMIUM_RECON", {"Expression": 2, "Lookup Procedure": 1}, 37),
    "wf_CLAIMS_FNOL_INTRADAY": ("INS_CLAIMS", "m_CLAIMS_FNOL_INTRADAY", {"Expression": 3, "Union Transformation": 1}, 37),
    "wf_PARTY_MDM_SYNC": ("MDM_PARTY", "m_PARTY_MDM_SYNC", {"Expression": 3}, 37),
    "wf_POLICY_MASTER_DAILY": ("INS_POLICY", "m_POLICY_MASTER_DAILY", {"Expression": 3, "Lookup Procedure": 1}, 101),
    "wf_REINSURANCE_BORDEREAUX_MONTHLY": ("RI_CESSIONS", "m_RI_BORDEREAUX_MONTHLY", {"Expression": 2, "Lookup Procedure": 1}, 34),
}

# Section 2.1: lookup tables are read edges (the most commonly missed one).
EXPECTED_LOOKUP_TABLES = {
    "wf_BILLING_PREMIUM_RECON": {"CORE_BANKING_DB.ACCOUNTS"},
    "wf_POLICY_MASTER_DAILY": {"REF_DB.XREF_CLIENT_PARTY"},
    "wf_REINSURANCE_BORDEREAUX_MONTHLY": {"REF_DB.SII_LOB_MAP"},
}

# Section 2.4: external scheduler edges (Control-M CMDLINE / cron wrapper).
EXPECTED_CONTROLM = {
    "wf_PARTY_MDM_SYNC": "ALB-MDM-0003",
    "wf_CLAIMS_FNOL_INTRADAY": "ALB-DWH-0047",
    "wf_BILLING_PREMIUM_RECON": "ALB-FIN-0012",
}
DUAL_OWNED = {"wf_POLICY_MASTER_DAILY": ("ALB-DWH-0032", "run_wf_policy_master")}   # trap 24

# Section 5/7 function inventory that the examples claim to exercise.
EXPECTED_FUNCTIONS = {
    "wf_BILLING_PREMIUM_RECON": {"DATE_DIFF", "LEAST", "GREATEST", "ROUND"},
    "wf_CLAIMS_FNOL_INTRADAY": {"IIF", "TO_DATE", "DECODE"},
    "wf_PARTY_MDM_SYNC": {"REG_MATCH", "SOUNDEX", "LOWER", "LTRIM", "RTRIM", "TO_DATE", "IIF", "ISNULL", "LENGTH", "SUBSTR"},
    "wf_POLICY_MASTER_DAILY": {"TO_INTEGER", "TO_DATE", "ADD_TO_DATE", "IIF", "SUBSTR", "REG_MATCH", "UPPER", "INSTR", "LTRIM", "RTRIM"},
    "wf_REINSURANCE_BORDEREAUX_MONTHLY": {"REPLACECHR", "REPLACESTR", "TO_DECIMAL", "CHR"},
}

FUNC_RE = re.compile(r"\b([A-Z_][A-Z0-9_]*)\s*\(")
PARAM_RE = re.compile(r"\$\$[A-Za-z_][A-Za-z0-9_]*")
SESSION_PARAM_RE = re.compile(r"(?<!\$)\$[A-Za-z_][A-Za-z0-9_]*")

report: list[str] = []
failures: list[str] = []


def check(cond: bool, msg: str) -> None:
    report.append(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        failures.append(msg)


def parse(path: Path) -> ET.Element:
    # Section 1.1: DTD disabled (powrmart.dtd never shipped), namespace-free; ElementTree does not fetch the DTD.
    return ET.fromstring(path.read_text(encoding="utf-8", errors="replace"))


def line_count(path: Path) -> int:
    return len(path.read_text(encoding="utf-8", errors="replace").splitlines())


def transformation_mix(mapping: ET.Element) -> Counter:
    return Counter(t.get("TYPE") for t in mapping.findall("TRANSFORMATION"))


def expressions(root: ET.Element) -> list[str]:
    out = []
    for f in root.iter("TRANSFORMFIELD"):
        e = f.get("EXPRESSION")
        if e and e != f.get("NAME"):
            out.append(e)
    for t in root.iter("TABLEATTRIBUTE"):
        if t.get("NAME") in ("Filter Condition", "Update Strategy Expression", "Group Filter Condition"):
            out.append(t.get("VALUE") or "")
    return out


def main(estate: Path) -> int:
    xml_dir = estate / "informatica" / "XML"
    par_dir = estate / "informatica" / "parameter_files"
    scripts = estate / "informatica" / "scripts"
    controlm = estate / "orchestration" / "controlm" / "ALBION_DWH_DAILY.xml"
    crontab = estate / "orchestration" / "cron" / "crontab_prod.txt"
    mapplet = estate / "informatica" / "mapplets" / "mplt_DQ_PARTY_STANDARDISE.xml"
    gss = estate / "informatica" / "legacy_shared_services" / "XML" / "wf_GSS_PAY_CALENDAR.xml"

    report.append("== Section 1: enumeration (POWERMART/REPOSITORY/FOLDER/{MAPPING,SESSION,WORKFLOW}) ==")
    lookups_by_wf: dict[str, set[str]] = {}
    funcs_by_wf: dict[str, set[str]] = {}
    params_by_wf: dict[str, set[str]] = {}
    for wf in WORKFLOW_XML:
        path = xml_dir / f"{wf}.xml"
        check(path.exists(), f"{wf}: export present at informatica/XML/{wf}.xml")
        if not path.exists():
            continue
        root = parse(path)
        check(root.tag == "POWERMART", f"{wf}: root element is POWERMART (got {root.tag})")
        folders = root.findall("./REPOSITORY/FOLDER")
        exp_folder, exp_mapping, exp_mix, exp_lines = EXPECTED_CENSUS[wf]
        check(len(folders) == 1 and folders[0].get("NAME") == exp_folder,
              f"{wf}: one FOLDER named {exp_folder} (got {[f.get('NAME') for f in folders]})")
        folder = folders[0]
        mappings = folder.findall("MAPPING")
        check([m.get("NAME") for m in mappings] == [exp_mapping],
              f"{wf}: census key {exp_folder}.{exp_mapping} (got {[m.get('NAME') for m in mappings]})")
        mix = transformation_mix(mappings[0])
        check(dict(mix) == exp_mix, f"{wf}: transformation type mix {dict(mix)} == section 1.4 {exp_mix}")
        check(line_count(path) == exp_lines, f"{wf}: XML lines {line_count(path)} == section 1.4 {exp_lines}")
        wfs = folder.findall("WORKFLOW")
        check(len(wfs) == 1 and wfs[0].get("NAME") == wf, f"{wf}: exactly one WORKFLOW with that name")
        sessions = wfs[0].findall("TASK[@TYPE='Session']") + wfs[0].findall("SESSION")
        check(len(sessions) >= 1, f"{wf}: >=1 session task/element found ({len(sessions)})")
        for s in wfs[0].iter("SESSION"):
            check(s.get("MAPPINGNAME") == exp_mapping, f"{wf}: SESSION@MAPPINGNAME binds to {exp_mapping}")
        reusable = [t.get("NAME") for t in mappings[0].findall("TRANSFORMATION[@REUSABLE='YES']")]
        report.append(f"     {wf}: reusable (shared-object) transformations = {reusable}")

        # Section 2.1 reads: SOURCE definitions, Source Qualifier overrides, lookup tables.
        sources = [s.get("NAME") for s in folder.findall("SOURCE")]
        targets = [t.get("NAME") for t in folder.findall("TARGET")]
        report.append(f"     {wf}: SOURCE={sources} TARGET={targets} (empty => lineage INFERRED from descriptions)")
        lk = set()
        for t in mappings[0].iter("TABLEATTRIBUTE"):
            if t.get("NAME") == "Lookup table name" and t.get("VALUE"):
                lk.add(t.get("VALUE"))
            if t.get("NAME") in ("Sql Query", "Lookup Sql Override", "Source Filter", "User Defined Join") and t.get("VALUE"):
                report.append(f"     {wf}: override {t.get('NAME')!r} = {t.get('VALUE')[:80]!r}")
        lookups_by_wf[wf] = lk
        if wf in EXPECTED_LOOKUP_TABLES:
            check(lk == EXPECTED_LOOKUP_TABLES[wf], f"{wf}: lookup read edges {sorted(lk)} == {sorted(EXPECTED_LOOKUP_TABLES[wf])}")

        # Section 5: function inventory from EXPRESSION attributes (XML-escaped values are unescaped by the parser).
        fn = set()
        for e in expressions(root):
            fn.update(FUNC_RE.findall(e))
        funcs_by_wf[wf] = fn
        missing = EXPECTED_FUNCTIONS[wf] - fn
        check(not missing, f"{wf}: expression functions cover section 12 claims (missing={sorted(missing)}; found={sorted(fn)})")

        # Section 2.3: $$ parameters referenced in the export.
        text = path.read_text(encoding="utf-8", errors="replace")
        params_by_wf[wf] = set(PARAM_RE.findall(text))
        report.append(f"     {wf}: $$ references in XML = {sorted(params_by_wf[wf])}")

        # Section 2.4 intra-workflow edges.
        links = [(l.get("FROMTASK"), l.get("TOTASK"), l.get("CONDITION") or "") for l in wfs[0].findall("WORKFLOWLINK")]
        check(any(f == "Start" for f, _, _ in links), f"{wf}: WORKFLOWLINK from Start present ({len(links)} links)")
        for f, t, c in links:
            if c:
                report.append(f"     {wf}: conditional link {f} -> {t} when {c!r}")
        for task in wfs[0].findall("TASK"):
            if task.get("TYPE") not in ("Start", "Session"):
                report.append(f"     {wf}: non-session TASK {task.get('NAME')} type={task.get('TYPE')}")

    # Policy Master specifics: fixed-width source, Event Wait, conditional email, Teradata target.
    pm = parse(xml_dir / "wf_POLICY_MASTER_DAILY.xml")
    ff = pm.find(".//SOURCE/FLATFILE")
    check(ff is not None and ff.get("DELIMITED") == "NO", "wf_POLICY_MASTER_DAILY: FLATFILE@DELIMITED=NO (fixed width, section 1.1)")
    check(any(f.get("PICTURETEXT") for f in pm.iter("SOURCEFIELD")), "wf_POLICY_MASTER_DAILY: PICTURETEXT implied-decimal fields present")
    check(pm.find(".//TASK[@TYPE='Event Wait']") is not None, "wf_POLICY_MASTER_DAILY: Event Wait task present (file edge)")
    check(pm.find(".//TASK[@TYPE='Email']") is not None, "wf_POLICY_MASTER_DAILY: Email task present")
    tgt = pm.find(".//TARGET")
    check(tgt is not None and tgt.get("DATABASETYPE") == "Teradata", "wf_POLICY_MASTER_DAILY: TARGET@DATABASETYPE=Teradata")
    check(pm.find(".//INSTANCE[@TRANSFORMATION_TYPE='Source Qualifier']") is not None
          and pm.find(".//TRANSFORMATION[@TYPE='Source Qualifier']") is None,
          "wf_POLICY_MASTER_DAILY: Source Qualifier present as INSTANCE only (abbreviated export, section 1.4)")
    check(len(pm.findall(".//CONNECTOR")) == 9, f"wf_POLICY_MASTER_DAILY: 9 CONNECTORs ({len(pm.findall('.//CONNECTOR'))})")
    check("49" in " ".join(expressions(pm)), "wf_POLICY_MASTER_DAILY: Julian pivot 49 literal in an expression (trap 22)")

    # Mapplet: shared object consumed by two workflows (section 3).
    report.append("== Section 3: shared objects ==")
    mp = parse(mapplet)
    mplt = mp.find(".//MAPPLET")
    check(mplt is not None and mplt.get("NAME") == "mplt_DQ_PARTY_STANDARDISE", "mapplet export contains MAPPLET mplt_DQ_PARTY_STANDARDISE")
    check(line_count(mapplet) == 18, f"mapplet XML lines {line_count(mapplet)} == section 1.4 18")
    desc = (mplt.get("DESCRIPTION") or "") if mplt is not None else ""
    check("wf_PARTY_MDM_SYNC" in desc and "wf_POLICY_MASTER_DAILY" in desc, "mapplet DESCRIPTION names both consuming workflows (2 consumers = shared, wave 0)")
    mfn = set()
    for e in expressions(mp):
        mfn.update(FUNC_RE.findall(e))
    check({"INITCAP", "REPLACESTR", "REPLACECHR"} <= mfn, f"mapplet functions include INITCAP/REPLACESTR/REPLACECHR ({sorted(mfn)})")

    # Section 2.3: parameter files - section header keying and $$ resolution.
    report.append("== Section 2.3: parameter files ==")
    for wf in ("wf_BILLING_PREMIUM_RECON", "wf_PARTY_MDM_SYNC", "wf_POLICY_MASTER_DAILY"):
        par = par_dir / f"{wf}.par"
        check(par.exists(), f"{wf}: parameter file present")
        if not par.exists():
            continue
        lines = par.read_text(encoding="utf-8", errors="replace").splitlines()
        headers = [l for l in lines if l.startswith("[")]
        check(any(f".{wf}." in h for h in headers), f"{wf}: [section] header keyed <repo>.<folder>.<workflow>.<session> ({headers})")
        declared = {l.split("=", 1)[0].strip() for l in lines if l.startswith("$$")}
        sess = {l.split("=", 1)[0].strip() for l in lines if l.startswith("$") and not l.startswith("$$")}
        referenced = params_by_wf.get(wf, set())
        unused = declared - referenced
        report.append(f"     {wf}: declared $$={sorted(declared)} session $={sorted(sess)} referenced-in-XML={sorted(referenced)}")
        if wf == "wf_BILLING_PREMIUM_RECON":
            check("$$IPT_RATE" in unused, "wf_BILLING_PREMIUM_RECON: $$IPT_RATE declared but unreferenced (finding, example 04)")
        if wf == "wf_POLICY_MASTER_DAILY":
            check("$$RUNDATE" in referenced and "$$RUNDATE" in declared, "wf_POLICY_MASTER_DAILY: $$RUNDATE declared and referenced (Event Wait path)")
            check(any(k.startswith("$DBConnection_") for k in sess), "wf_POLICY_MASTER_DAILY: $DBConnection_* names resolve connections (values are names, not secrets)")

    # Section 2.4: external scheduler edges.
    report.append("== Section 2.4: scheduler edges (Control-M, cron, wrappers) ==")
    cm = parse(controlm)
    jobs = {j.get("JOBNAME"): j for j in cm.iter("JOB")}
    for wf, jobname in EXPECTED_CONTROLM.items():
        j = jobs.get(jobname)
        check(j is not None and f"pmcmd startworkflow -f " in (j.get("CMDLINE") or "") and wf in (j.get("CMDLINE") or ""),
              f"{wf}: Control-M {jobname} CMDLINE starts it via pmcmd")
    j47 = jobs["ALB-DWH-0047"]
    check(j47.get("CYCLIC") == "Y" and j47.get("INTERVAL") == "30M", "wf_CLAIMS_FNOL_INTRADAY: CYCLIC=Y INTERVAL=30M (windowed cyclic)")
    check(jobs["ALB-FIN-0012"].get("MONTHDAYS") == "WD1", "wf_BILLING_PREMIUM_RECON: MONTHDAYS=WD1 business-calendar rule (no cron equivalent)")
    j32 = jobs["ALB-DWH-0032"]
    check("run_wf_policy_master" in (j32.get("CMDLINE") or ""), "wf_POLICY_MASTER_DAILY: Control-M ALB-DWH-0032 runs the wrapper, not pmcmd directly")
    check([c.get("NAME") for c in j32.findall("INCOND")] == ["ALB-DWH-0031-OK"], "wf_POLICY_MASTER_DAILY: INCOND from NDM watch only (no MDM dependency = race finding)")
    cron = crontab.read_text(encoding="utf-8", errors="replace")
    check("run_wf_policy_master" in cron, "wf_POLICY_MASTER_DAILY: cron also starts the wrapper => dual ownership (trap 24)")
    check(re.search(r"^0\s+6\s+1-5\s+\*\s+\*\s+\S*bdx_transfer", cron, re.M) is not None, "bdx_transfer: cron 06:00 days 1-5 lands broker files")
    check("wf_REINSURANCE_BORDEREAUX_MONTHLY" not in cron and not any("wf_REINSURANCE_BORDEREAUX_MONTHLY" in (j.get("CMDLINE") or "") for j in jobs.values()),
          "wf_REINSURANCE_BORDEREAUX_MONTHLY: not started by either scheduler export => INFERRED manual/pmcmd start")
    wrapper = (scripts / "run_wf_policy_master").read_text(encoding="utf-8", errors="replace")
    check("pmcmd startworkflow" in wrapper and "-f INS_POLICY" in wrapper and "wf_POLICY_MASTER_DAILY" in wrapper, "run_wf_policy_master: pmcmd startworkflow -f INS_POLICY wf_POLICY_MASTER_DAILY")
    check("if [ ! -e $FNAME ]" in wrapper, "run_wf_policy_master: file pre-condition guard is an inbound file edge")
    check("run_insurance_bteq.sh" in wrapper, "run_wf_policy_master: kicks run_insurance_bteq.sh after the workflow (cross-engine edge)")
    check("$pmpasswd" in wrapper, "run_wf_policy_master: password from env var (reference by name only; value never copied)")
    bdx = (scripts / "bdx_transfer").read_text(encoding="utf-8", errors="replace")
    check("sftp" in bdx and "mailx" in bdx, "bdx_transfer: SFTP pull + mailx completion (pre-step wrapper)")

    # Section 1.1 / 6: full-fidelity session shape from the GSS export.
    report.append("== Sections 1.1/6: session-layer element shapes (legacy_shared_services) ==")
    g = parse(gss)
    check(g.find(".//SESSIONEXTENSION[@TYPE='READER']") is not None and g.find(".//SESSIONEXTENSION[@TYPE='WRITER']") is not None,
          "wf_GSS_PAY_CALENDAR: READER and WRITER SESSIONEXTENSIONs present")
    check(g.find(".//CONNECTIONREFERENCE") is not None, "wf_GSS_PAY_CALENDAR: CONNECTIONREFERENCE present (connection names, section 9)")
    comp_types = {c.get("TYPE") for c in g.iter("SESSIONCOMPONENT")}
    check("Failure Email" in comp_types and "Pre-session variable assignment" in comp_types, f"wf_GSS_PAY_CALENDAR: SESSIONCOMPONENT types {sorted(comp_types)}")
    attrs = {a.get("NAME"): a.get("VALUE") for s in g.iter("SESSION") for a in s.findall("ATTRIBUTE")}
    for name in ("Treat source rows as", "Commit Interval", "Recovery Strategy", "Commit Type", "Rollback Transactions on Errors"):
        check(name in attrs, f"wf_GSS_PAY_CALENDAR: session ATTRIBUTE {name!r} present")
    check(any(t.get("NAME") == "Update Strategy Expression" and t.get("VALUE") == "DD_UPDATE" for t in g.iter("TABLEATTRIBUTE")),
          "wf_GSS_PAY_CALENDAR: Update Strategy Expression DD_UPDATE present")
    check(any(t.get("NAME") == "Source Filter" and t.get("VALUE") for t in g.iter("TABLEATTRIBUTE")), "wf_GSS_PAY_CALENDAR: non-empty Source Filter override present")
    check(any(t.get("NAME") in ("Sql Query", "Lookup Sql Override") and t.get("VALUE") for t in g.iter("TABLEATTRIBUTE")),
          "wf_GSS_PAY_CALENDAR: non-empty Sql Query / Lookup Sql Override present somewhere in the export")
    check(any((l.get("CONDITION") or "").strip() for l in g.iter("WORKFLOWLINK")), "wf_GSS_PAY_CALENDAR: conditional WORKFLOWLINKs present")
    check(g.find(".//WORKFLOWVARIABLE") is not None, "wf_GSS_PAY_CALENDAR: WORKFLOWVARIABLE elements present")

    print("FIXTURE VALIDATION (XML/text only; no live PowerCenter, source DB, or Databricks access)")
    print("\n".join(report))
    print(f"\n{len([r for r in report if r.startswith('PASS')])} passed, {len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(Path(sys.argv[1])))
