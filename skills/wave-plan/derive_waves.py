"""Derive leaf-first wave batches from unit dependency analyses.
The manager pastes the derived batches into the hand-edited wave manifest.
"""

import argparse
import json
import sys
from collections import Counter, deque
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "migration-fanout"))
from manifest import mapped_target, other_wave_manifests, target_key, transitive_writes, valid_namespace


def unit_rows(path):
    try:
        data = json.loads(path.read_text())
    except ValueError as e:
        raise SystemExit(f"{path} is not valid JSON ({e})") from None
    rows = data.get("routines") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not all(
            isinstance(r, dict) and isinstance(r.get("routine"), str) and r["routine"]
            and all(isinstance(r.get(k), list) and all(isinstance(t, str) and t for t in r[k])
                    for k in ("reads", "writes", "calls"))
            for r in rows):
        raise SystemExit(f"{path} needs a 'routines' list of {{routine, reads, writes, calls}} rows "
                         "(string name, lists of names)")
    names = Counter(r["routine"].casefold() for r in rows)
    if any(n > 1 for n in names.values()):
        raise SystemExit(f"{path} names a routine twice: "
                         f"{', '.join(sorted(k for k, n in names.items() if n > 1))}")
    return rows


def _strongly_connected(units, dependencies):
    index, lowlink = {}, {}
    pending, on_pending = [], set()
    components = []
    next_index = 0

    for root in units:
        if root in index:
            continue
        index[root] = lowlink[root] = next_index
        next_index += 1
        pending.append(root)
        on_pending.add(root)
        search = [(root, iter(sorted(dependencies[root])))]

        while search:
            unit, neighbors = search[-1]
            try:
                neighbor = next(neighbors)
            except StopIteration:
                search.pop()
                if search:
                    parent = search[-1][0]
                    lowlink[parent] = min(lowlink[parent], lowlink[unit])
                if lowlink[unit] == index[unit]:
                    component = []
                    while True:
                        member = pending.pop()
                        on_pending.remove(member)
                        component.append(member)
                        if member == unit:
                            break
                    components.append(tuple(sorted(component)))
                continue

            if neighbor not in index:
                index[neighbor] = lowlink[neighbor] = next_index
                next_index += 1
                pending.append(neighbor)
                on_pending.add(neighbor)
                search.append((neighbor, iter(sorted(dependencies[neighbor]))))
            elif neighbor in on_pending:
                lowlink[unit] = min(lowlink[unit], index[neighbor])

    return sorted(components)


def _depths(components, dependencies):
    component_of = {unit: i for i, component in enumerate(components) for unit in component}
    needs = {i: set() for i in range(len(components))}
    dependents = {i: [] for i in range(len(components))}
    for unit, targets in dependencies.items():
        source = component_of[unit]
        for target in targets:
            dest = component_of[target]
            if source != dest:
                needs[source].add(dest)
                dependents[dest].append(source)

    remaining = {i: len(targets) for i, targets in needs.items()}
    deepest = {i: -1 for i in needs}
    ready = deque(sorted(i for i, count in remaining.items() if count == 0))
    depths = {}
    while ready:
        component = ready.popleft()
        depths[component] = deepest[component] + 1
        for dependent in sorted(dependents[component]):
            deepest[dependent] = max(deepest[dependent], depths[component])
            remaining[dependent] -= 1
            if remaining[dependent] == 0:
                ready.append(dependent)
    return component_of, depths


def derive(root, namespace="", batch_max=5, max_cycle=4, first_wave=1, shared_wave_0=False):
    root = Path(root)
    units_root = root / ".migration" / "units"
    try:
        unit_dirs = sorted((path for path in units_root.iterdir() if path.is_dir()
                            and (path / "dependencies.json").is_file()), key=lambda path: path.name)
    except FileNotFoundError:
        unit_dirs = []
    except OSError as e:
        raise SystemExit(f"cannot read {units_root}: {e}") from None
    if not unit_dirs:
        raise SystemExit(f"no unit dependencies found at {units_root}/*/dependencies.json; "
                         "the dialect skill emits them")

    rows_by_unit = {path.name: unit_rows(path / "dependencies.json") for path in unit_dirs}
    owner, routine_names = {}, {}
    for unit, rows in rows_by_unit.items():
        for row in rows:
            key = row["routine"].casefold()
            if key in owner:
                prior_unit = owner[key]
                routine = routine_names[key]
                raise SystemExit(f"routine {routine} is analysed by units {prior_unit}, {unit}; "
                                 "one routine belongs to one unit's dependencies.json, then re-run")
            owner[key] = unit
            routine_names[key] = row["routine"]

    called_units = {unit: set() for unit in rows_by_unit}
    for unit, rows in rows_by_unit.items():
        for row in rows:
            for callee in row["calls"]:
                callee_owner = owner.get(callee.casefold())
                if callee_owner is None:
                    raise SystemExit(f"unit {unit}: routine {row['routine']} calls {callee}, which the dependency "
                                     "analysis does not cover, so its writes are unknown")
                if callee_owner != unit:
                    called_units[unit].add(callee_owner)

    call_closures = {}
    for unit in rows_by_unit:
        reachable, todo = set(), [unit]
        while todo:
            current = todo.pop()
            if current in reachable:
                continue
            reachable.add(current)
            todo.extend(called_units[current] - reachable)
        call_closures[unit] = [
            row for owner_unit in sorted(reachable) for row in rows_by_unit[owner_unit]
        ]

    specs = {}
    for path in unit_dirs:
        spec_path = path / "mapping_spec.json"
        rows = rows_by_unit[path.name]
        if not spec_path.is_file():
            tables = sorted({table for row in rows for table in row["writes"]})
            if tables:
                raise SystemExit(f"unit {path.name} writes {tables} but has no "
                                 f".migration/units/{path.name}/mapping_spec.json, so its targets are unknown; "
                                 "write the mapping spec, then re-run")
            specs[path.name] = None
            continue
        try:
            specs[path.name] = json.loads(spec_path.read_text())
        except ValueError as e:
            raise SystemExit(f"mapping spec {spec_path} is not valid JSON ({e})") from None

    writes = {}
    for unit, rows in call_closures.items():
        def resolve(routine, table):
            owner_unit = owner[routine["routine"].casefold()]
            targets = mapped_target(specs[owner_unit], table, namespace)
            if not targets:
                raise SystemExit(f"unit {owner_unit}'s routine {routine['routine']} writes '{table}', which no "
                                 "object of its mapping_spec.json has as root_table, so its target is unknown")
            return {target_key(target, namespace) for target in targets}

        try:
            writes[unit] = set(transitive_writes(rows, resolve))
        except SystemExit as e:
            raise SystemExit(f"unit {unit}: {e}") from None

    reads = {}
    for unit, rows in rows_by_unit.items():
        targets = set()
        for row in rows:
            for table in row["reads"]:
                for spec in specs.values():
                    if spec is not None:
                        targets.update(mapped_target(spec, table, namespace))
        reads[unit] = targets

    dependencies = {unit: set() for unit in rows_by_unit}
    for unit, callees in called_units.items():
        for callee in callees:
            dependencies[unit].add(callee)
            dependencies[callee].add(unit)
    for unit in rows_by_unit:
        for other in rows_by_unit:
            if unit != other and (reads[unit] & writes[other] or writes[unit] & writes[other]):
                dependencies[unit].add(other)

    components = _strongly_connected(sorted(rows_by_unit), dependencies)
    for component in components:
        if len(component) > max_cycle:
            raise SystemExit(f"dependency cycle of {len(component)} units exceeds --max-cycle {max_cycle}: "
                             f"{', '.join(component)}; break the cycle in the analysis or raise --max-cycle, "
                             "then re-run")
        if len(component) > batch_max:
            raise SystemExit(f"dependency cycle of {len(component)} units exceeds --batch-max {batch_max}: "
                             f"{', '.join(component)}; an SCC cannot be split; raise --batch-max, then re-run")

    readers = {
        unit: sorted(other for other in rows_by_unit
                     if other != unit and reads[other] & writes[unit])
        for unit in rows_by_unit
    }
    shared = {unit for unit in rows_by_unit if len(readers[unit]) >= 2}
    wave_zero = {unit for unit in shared if not dependencies[unit]} if shared_wave_0 else set()
    normal_units = sorted(set(rows_by_unit) - wave_zero)
    normal_dependencies = {
        unit: dependencies[unit] & set(normal_units)
        for unit in normal_units
    }
    normal_components = _strongly_connected(normal_units, normal_dependencies)
    _, depths = _depths(normal_components, normal_dependencies)
    unit_results = {
        unit: {
            "reads": sorted(reads[unit]),
            "writes": sorted(writes[unit]),
            "depends_on": sorted(dependencies[unit]),
        }
        for unit in sorted(rows_by_unit)
    }
    by_wave = {}
    for i, component in enumerate(normal_components):
        by_wave.setdefault(first_wave + depths[i], []).append(component)

    waves = []
    if wave_zero:
        waves.append({"wave": 0, "batches": [
            {"id": f"w0-b{number:02d}", "units": [unit], "write_targets": sorted(writes[unit])}
            for number, unit in enumerate(sorted(wave_zero), 1)
        ]})
    for wave in sorted(by_wave):
        groups = sorted(by_wave[wave], key=lambda component: (len(component), component))
        batches, current = [], []

        def flush():
            if not current:
                return
            number = len(batches) + 1
            batch_units = sorted(unit for component in current for unit in component)
            targets = sorted({target for unit in batch_units for target in writes[unit]})
            batches.append({"id": f"w{wave}-b{number:02d}", "units": batch_units, "write_targets": targets})
            current.clear()

        size = 0
        for component in groups:
            if current and size + len(component) > batch_max:
                flush()
                size = 0
            current.append(component)
            size += len(component)
        flush()
        waves.append({"wave": wave, "batches": batches})

    result = {"units": unit_results, "waves": waves}
    if shared_wave_0:
        result["shared_not_wave_0"] = [
            {"unit": unit, "readers": readers[unit],
             "reason": f"depends on {', '.join(sorted(dependencies[unit]))}"}
            for unit in sorted(shared - wave_zero)
        ]
    return result


def _check_path(root, manifest_path):
    path = Path(manifest_path)
    return path if path.is_absolute() else Path(root) / path


def _read_check_manifest(path):
    try:
        manifest = json.loads(path.read_text())
    except ValueError as e:
        raise SystemExit(f"{path} is not valid JSON ({e})") from None
    if (not isinstance(manifest, dict) or not isinstance(manifest.get("wave"), int)
            or isinstance(manifest.get("wave"), bool)
            or not isinstance(manifest.get("batches"), list)
            or not all(isinstance(batch, dict) and isinstance(batch.get("id"), str)
                       and isinstance(batch.get("units"), list)
                       and all(isinstance(unit, str) for unit in batch["units"])
                       for batch in manifest["batches"])):
        raise SystemExit(f"{path} needs an integer 'wave' and a 'batches' list of {{id, units}} rows")
    return manifest


def check(root, manifest_path, derived):
    path = _check_path(root, manifest_path)
    manifest = _read_check_manifest(path)
    placements, current = {}, {}

    def record(batch, wave, filename, is_current):
        for unit in batch["units"]:
            placement = (wave, filename, batch["id"])
            placements[unit] = placement
            if is_current:
                current[unit] = placement

    for batch in manifest["batches"]:
        record(batch, manifest["wave"], path.name, True)

    siblings = other_wave_manifests(path.parent, path.name)
    for filename, sibling in siblings.items():
        sibling_path = path.parent / filename
        try:
            sibling_manifest = json.loads(sibling_path.read_text())
        except ValueError as e:
            raise SystemExit(f"{sibling_path} is not valid JSON ({e})") from None
        wave = sibling_manifest.get("wave") if isinstance(sibling_manifest, dict) else None
        if not isinstance(wave, int) or isinstance(wave, bool):
            raise SystemExit(f"{sibling_path} needs an integer 'wave'")
        for batch in sibling["batches"]:
            record(batch, wave, filename, False)

    findings = []
    components = _strongly_connected(sorted(derived["units"]),
                                     {unit: set(data["depends_on"])
                                      for unit, data in derived["units"].items()})
    for component in components:
        if len(component) <= 1 or not any(unit in current for unit in component):
            continue
        places = {current.get(unit) for unit in component}
        if None in places or len(places) != 1:
            details = ", ".join(
                f"{unit}=wave {current[unit][0]} {current[unit][1]}/{current[unit][2]}"
                if unit in current else f"{unit}=not in {path.name}"
                for unit in component)
            findings.append(f"SCC units {', '.join(component)} are split across batches: {details}")

    for unit, data in derived["units"].items():
        for dependency in data["depends_on"]:
            if unit not in current and dependency not in current:
                continue
            source, target = placements.get(unit), placements.get(dependency)
            if source is None or target is None:
                if source is not None:
                    source_label = f"wave {source[0]} {source[1]}/{source[2]}"
                    findings.append(f"dependency {unit} ({source_label}) -> {dependency} is not placed in any "
                                    "wave manifest")
                continue
            source_label = f"wave {source[0]} {source[1]}/{source[2]}"
            target_label = f"wave {target[0]} {target[1]}/{target[2]}"
            if source[0] == target[0] and source[1:] != target[1:]:
                findings.append(f"dependency {unit} ({source_label}) -> {dependency} ({target_label}) "
                                "is split across batches in the same wave")
            elif target[0] > source[0]:
                findings.append(f"dependency {unit} ({source_label}) -> {dependency} ({target_label}) "
                                "is placed in a later wave")
    return findings


def main(argv=None):
    parser = argparse.ArgumentParser(description="Derive leaf-first unit wave batches from dependency analyses.")
    parser.add_argument("--root", required=True)
    parser.add_argument("--namespace")
    parser.add_argument("--batch-max", type=int, default=5)
    parser.add_argument("--max-cycle", type=int, default=4)
    parser.add_argument("--first-wave", type=int, default=1)
    parser.add_argument("--shared-wave-0", action="store_true")
    parser.add_argument("--out")
    parser.add_argument("--check")
    args = parser.parse_args(argv)

    if args.namespace is not None and not valid_namespace(args.namespace):
        raise SystemExit("--namespace must be a dotted catalog.schema (or schema)")
    if args.batch_max < 1:
        raise SystemExit("--batch-max must be at least 1")
    if args.max_cycle < 1:
        raise SystemExit("--max-cycle must be at least 1")
    if args.first_wave == 0:
        raise SystemExit("wave 0 is the serial shared-objects wave; pass --shared-wave-0 instead")
    if args.first_wave < 1:
        raise SystemExit("--first-wave must be at least 1")

    root = Path(args.root).resolve()
    check_path = _check_path(root, args.check) if args.check else None
    check_manifest = _read_check_manifest(check_path) if check_path else None
    namespace = args.namespace
    if namespace is None and check_manifest is not None:
        namespace = check_manifest.get("target_namespace", "")
        if namespace != "" and not valid_namespace(namespace):
            raise SystemExit(f"{check_path} 'target_namespace' must be a dotted catalog.schema (or schema)")
    namespace = namespace or ""

    derived = derive(root, namespace, args.batch_max, args.max_cycle, args.first_wave, args.shared_wave_0)

    out_path = Path(args.out).resolve() if args.out else None
    migration_dir = (root / ".migration").resolve()
    if out_path is not None and (out_path == migration_dir or migration_dir in out_path.parents):
        raise SystemExit(".migration/ holds only files a tool reads and the wave manifest is hand-edited from this "
                         "output; write it outside .migration/ (e.g. /tmp/waves.json)")

    if out_path is not None:
        try:
            out_path.write_text(json.dumps(derived, indent=2, sort_keys=True) + "\n")
        except OSError as e:
            raise SystemExit(f"cannot write {out_path}: {e}") from None

    if check_path is not None:
        findings = check(root, check_path, derived)
        for finding in findings:
            print(finding)
        if not findings:
            print(f"{check_path}: consistent with the derived dependency order")
        return 1 if findings else 0

    if out_path is None:
        print(json.dumps(derived, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
