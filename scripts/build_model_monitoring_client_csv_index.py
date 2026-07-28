from __future__ import annotations

import argparse
import csv
from pathlib import Path


TABLE_FILES = {
    "processlog.csv": "processlog.csv",
    "sessionquery.csv": "sessionquery.csv",
    "formenginelog.csv": "formenginelog.csv",
    "cealog.csv": "cealog.csv",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build per-client CSV shards for model monitoring load tests.")
    parser.add_argument("--input-dir", default="data/model_monitoring/load_test")
    parser.add_argument("--output-dir", default="data/model_monitoring/load_test/by_client")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for source_name, target_name in TABLE_FILES.items():
        split_table(input_dir / source_name, output_dir, target_name)
    print(f"Wrote per-client CSV index to {output_dir}")


def split_table(source_path: Path, output_dir: Path, target_name: str) -> None:
    handles: dict[str, object] = {}
    writers: dict[str, csv.DictWriter] = {}
    try:
        with source_path.open("r", newline="", encoding="utf-8-sig") as source:
            reader = csv.DictReader(source)
            if not reader.fieldnames:
                return
            for row in reader:
                clientid = row.get("clientid") or row.get("CLIENTID") or ""
                clientname = row.get("clientname") or row.get("CLIENTNAME") or ""
                slug = client_slug(clientid=clientid, clientname=clientname)
                if not slug:
                    continue
                writer = writers.get(slug)
                if writer is None:
                    client_dir = output_dir / slug
                    client_dir.mkdir(parents=True, exist_ok=True)
                    handle = (client_dir / target_name).open("w", newline="", encoding="utf-8")
                    handles[slug] = handle
                    writer = csv.DictWriter(handle, fieldnames=reader.fieldnames)
                    writer.writeheader()
                    writers[slug] = writer
                writer.writerow(row)
    finally:
        for handle in handles.values():
            handle.close()


def client_slug(*, clientid: str, clientname: str) -> str:
    raw = "_".join(value for value in [clientid.strip(), clientname.strip()] if value)
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in raw).strip("_")


if __name__ == "__main__":
    main()
