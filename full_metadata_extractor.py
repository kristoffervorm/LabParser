import json
import hashlib
from pathlib import Path
from datetime import datetime
from fcsparser import parse


def sha256_of_file(path):
    """Compute SHA-256 checksum of the raw FCS file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_lasers(meta):
    """Extract laser configuration from Attune NxT metadata."""
    lasers = {}
    for i in range(1, 5):
        lasers[f"laser{i}"] = {
            "color": meta.get(f"#LASER{i}COLOR"),
            "ASF":   safe_float(meta.get(f"#LASER{i}ASF")),
            "delay": safe_int(meta.get(f"#LASER{i}DELAY"))
        }
    return lasers


def extract_reagents(meta):
    """
    Extract all Panel annotation pairs: (Label, Target)
    from keys like #P4Label, #P4Target, #P10Label, etc.
    Deduplicate and return a clean list.
    """
    reagents = []
    for n in range(1, 30):
        label = meta.get(f"#P{n}Label")
        target = meta.get(f"#P{n}Target")

        if label and target and label != "NA" and target != "NA":
            reagents.append({
                "name": f"{label} ({target})",
                "lot_number": ""
            })

    # remove duplicates
    unique = {item["name"]: item for item in reagents}
    return list(unique.values())


def safe_float(x):
    try:
        return float(x)
    except:
        return None

def safe_int(x):
    try:
        return int(x)
    except:
        return None


def fcs_time_to_utc(date_str, time_str):
    """
    Convert DATE + BTIM from FCS into UTC (if possible).
    Attune gives DATE in “17-Mar-2026” and time “15:56:43”.
    We’ll return ISO-8601; user can later convert timezone.
    """
    if not date_str or not time_str:
        return ""

    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%d-%b-%Y %H:%M:%S")
        return dt.isoformat() + "Z"
    except:
        return ""


def generate_full_metadata(fcs_path, schema_path="MetadataSchema.json"):

    # 1. Load schema template
    with open(schema_path, "r") as f:
        schema = json.load(f)

    # 2. Parse FCS
    meta, data = parse(fcs_path, reformat_meta=True)

    # -------------------------------------------------------------------------
    # IDENTIFICATION BLOCK
    # -------------------------------------------------------------------------
    schema["identification"]["result"] = Path(fcs_path).name
    schema["identification"]["project"] = meta.get("$PROJ") or ""
    schema["identification"]["experiment"] = meta.get("$PLATENAME") or ""
    schema["identification"]["run"] = meta.get("$FIL") or ""
    schema["identification"]["sample"] = meta.get("$SMNO") or ""
    # (Other ID fields left empty intentionally)

    # -------------------------------------------------------------------------
    # INSTRUMENT ACQUISITION BLOCK
    # -------------------------------------------------------------------------
    schema["instrument_acquisition"]["instrument_id"] = meta.get("$CYTSN")
    schema["instrument_acquisition"]["instrument_type"] = "Flow cytometer"
    schema["instrument_acquisition"]["instrument_model"] = meta.get("$CYT")
    schema["instrument_acquisition"]["instrument_run"] = meta.get("$FIL")

    schema["instrument_acquisition"]["instrument_settings"] = {
        "flow_rate": safe_float(meta.get("#FLOWRATE")),
        "timestep": safe_float(meta.get("$TIMESTEP")),
        "coincident_count": safe_int(meta.get("#CoincidentCount")),
        "lasers": parse_lasers(meta),
        "laser_configuration": meta.get("#LASERCONFIG"),
        "trigger": {
            "channel": meta.get("#TR1").split(",")[0] if meta.get("#TR1") else None,
            "threshold": safe_int(meta.get("#TR1").split(",")[1]) if meta.get("#TR1") else None,
            "width_threshold": safe_int(meta.get("#WIDTHTHRESHOLD")),
            "window_extension": safe_int(meta.get("#WINEXT"))
        }
    }

    # Acquisition timestamp
    schema["instrument_acquisition"]["acquisition_timestamp_utc"] = \
        fcs_time_to_utc(meta.get("$DATE"), meta.get("$BTIM"))

    # File checksum
    schema["instrument_acquisition"]["checksum_sha256"] = sha256_of_file(fcs_path)

    # -------------------------------------------------------------------------
    # BIOLOGICAL CONTEXT BLOCK
    # -------------------------------------------------------------------------
    schema["biological_context"]["operator"] = meta.get("$OP")
    schema["biological_context"]["data_intent"] = "Compensation control"

    # -------------------------------------------------------------------------
    # REAGENTS BLOCK (auto-extracted from P#Label / P#Target)
    # -------------------------------------------------------------------------
    schema["reagents"] = extract_reagents(meta)

    # -------------------------------------------------------------------------
    # DATA QUALITY BLOCK
    # -------------------------------------------------------------------------
    schema["data_quality"]["measurement_status"] = meta.get("#PTRESULT")
    schema["data_quality"]["data_level"] = "Raw"

    # -------------------------------------------------------------------------
    # WRITE TO FILE
    # -------------------------------------------------------------------------
    out_path = Path(fcs_path).with_suffix(".metadata.json")
    with open(out_path, "w") as f:
        json.dump(schema, f, indent=2)

    print(f"✅ Full metadata written to {out_path}")
    return out_path
