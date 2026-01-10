#!/usr/bin/env python3
import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from xml.etree.ElementTree import iterparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream an Apple Health export.xml and summarize types/sources/units."
    )
    parser.add_argument(
        "export",
        nargs="?",
        default="export.xml",
        help="Path to export.xml (default: export.xml)",
    )
    parser.add_argument(
        "--out",
        default="inventory.json",
        help="Output JSON path (default: inventory.json)",
    )
    parser.add_argument(
        "--max-elements",
        type=int,
        default=0,
        help="Stop after processing this many elements (0 = no limit).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    totals = Counter()
    record_types = Counter()
    record_sources = Counter()
    record_devices = Counter()
    record_units_by_type = defaultdict(Counter)
    record_source_versions = defaultdict(Counter)

    workout_types = Counter()
    workout_sources = Counter()
    workout_devices = Counter()

    correlation_types = Counter()
    activity_summary_sources = Counter()
    clinical_record_types = Counter()
    audiogram_sources = Counter()
    vision_sources = Counter()

    processed = 0

    try:
        for _event, elem in iterparse(args.export, events=("end",)):
            tag = elem.tag
            if tag == "Record":
                totals["Record"] += 1
                attrs = elem.attrib
                rtype = attrs.get("type", "Unknown")
                record_types[rtype] += 1
                unit = attrs.get("unit") or "None"
                record_units_by_type[rtype][unit] += 1
                source = attrs.get("sourceName", "Unknown")
                record_sources[source] += 1
                device = attrs.get("device")
                if device:
                    record_devices[device] += 1
                version = attrs.get("sourceVersion")
                if version:
                    record_source_versions[source][version] += 1
            elif tag == "Workout":
                totals["Workout"] += 1
                attrs = elem.attrib
                wtype = attrs.get("workoutActivityType", "Unknown")
                workout_types[wtype] += 1
                source = attrs.get("sourceName", "Unknown")
                workout_sources[source] += 1
                device = attrs.get("device")
                if device:
                    workout_devices[device] += 1
            elif tag == "Correlation":
                totals["Correlation"] += 1
                ctype = elem.attrib.get("type", "Unknown")
                correlation_types[ctype] += 1
            elif tag == "ActivitySummary":
                totals["ActivitySummary"] += 1
                source = elem.attrib.get("sourceName", "Unknown")
                activity_summary_sources[source] += 1
            elif tag == "ClinicalRecord":
                totals["ClinicalRecord"] += 1
                ctype = elem.attrib.get("type", "Unknown")
                clinical_record_types[ctype] += 1
            elif tag == "Audiogram":
                totals["Audiogram"] += 1
                source = elem.attrib.get("sourceName", "Unknown")
                audiogram_sources[source] += 1
            elif tag == "VisionPrescription":
                totals["VisionPrescription"] += 1
                source = elem.attrib.get("sourceName", "Unknown")
                vision_sources[source] += 1

            elem.clear()
            processed += 1
            if args.max_elements and processed >= args.max_elements:
                break
    except FileNotFoundError:
        print(f"File not found: {args.export}", file=sys.stderr)
        return 2

    def counter_to_dict(counter):
        return dict(counter.most_common())

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "export_path": args.export,
        "totals": counter_to_dict(totals),
        "record_types": counter_to_dict(record_types),
        "record_sources": counter_to_dict(record_sources),
        "record_devices": counter_to_dict(record_devices),
        "record_units_by_type": {
            rtype: counter_to_dict(units) for rtype, units in record_units_by_type.items()
        },
        "record_source_versions": {
            source: counter_to_dict(versions)
            for source, versions in record_source_versions.items()
        },
        "workout_types": counter_to_dict(workout_types),
        "workout_sources": counter_to_dict(workout_sources),
        "workout_devices": counter_to_dict(workout_devices),
        "correlation_types": counter_to_dict(correlation_types),
        "activity_summary_sources": counter_to_dict(activity_summary_sources),
        "clinical_record_types": counter_to_dict(clinical_record_types),
        "audiogram_sources": counter_to_dict(audiogram_sources),
        "vision_sources": counter_to_dict(vision_sources),
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)

    print(f"Wrote {args.out}")
    print(f"Totals: {out['totals']}")
    print(f"Record types: {len(record_types)}")
    print(f"Workout types: {len(workout_types)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
