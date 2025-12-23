"""Output writer for generating CSV files."""

import csv
import json
import hashlib
from datetime import datetime
from pathlib import Path


class OutputWriter:
    """Writes processed IP data to various output formats."""

    def __init__(self, output_dir):
        """Initialize output writer.

        Args:
            output_dir: Directory to write output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def _calculate_sha256(self, filepath):
        """Calculate SHA256 hash of a file."""
        hash_sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def write_aggregated_csv_files(self, ipv4_agg_entries, ipv6_agg_entries):
        """Write only aggregated CSV files for performance."""
        print("Writing aggregated CSV files...")

        files_info = {}

        # Write IPv4 aggregated
        ipv4_agg_file = self.output_dir / "prefixes_ipv4_agg.csv"
        with open(ipv4_agg_file, "w", newline="") as f:
            writer = csv.writer(f)
            for prefix, cc in sorted(
                ipv4_agg_entries, key=lambda x: x[0].network_address
            ):
                writer.writerow([str(prefix), cc])

        files_info["prefixes_ipv4_agg.csv"] = {
            "path": str(ipv4_agg_file),
            "size": ipv4_agg_file.stat().st_size,
            "sha256": self._calculate_sha256(ipv4_agg_file),
            "count": len(ipv4_agg_entries),
        }

        # Write IPv6 aggregated
        ipv6_agg_file = self.output_dir / "prefixes_ipv6_agg.csv"
        with open(ipv6_agg_file, "w", newline="") as f:
            writer = csv.writer(f)
            for prefix, cc in sorted(
                ipv6_agg_entries, key=lambda x: x[0].network_address
            ):
                writer.writerow([str(prefix), cc])

        files_info["prefixes_ipv6_agg.csv"] = {
            "path": str(ipv6_agg_file),
            "size": ipv6_agg_file.stat().st_size,
            "sha256": self._calculate_sha256(ipv6_agg_file),
            "count": len(ipv6_agg_entries),
        }

        print(f"  IPv4 aggregated: {len(ipv4_agg_entries):,} prefixes")
        print(f"  IPv6 aggregated: {len(ipv6_agg_entries):,} prefixes")

        return files_info

    def _serialize_conflicts(self, conflicts):
        """Convert datetime.date objects to ISO strings for JSON serialization."""
        if not conflicts:
            return []
        serialized = []
        for conflict in conflicts:
            serialized.append(
                {
                    "prefix": conflict["prefix"],
                    "entries": [
                        (
                            reg,
                            cc,
                            (
                                date.isoformat()
                                if hasattr(date, "isoformat")
                                else str(date)
                            ),
                        )
                        for reg, cc, date in conflict["entries"]
                    ],
                    "chosen": (
                        conflict["chosen"][0],
                        conflict["chosen"][1],
                        (
                            conflict["chosen"][2].isoformat()
                            if hasattr(conflict["chosen"][2], "isoformat")
                            else str(conflict["chosen"][2])
                        ),
                    ),
                }
            )
        return serialized

    def write_metadata(self, download_metadata, files_info, conflicts=None):
        """Write metadata JSON file."""
        print("Writing metadata...")

        metadata = {
            "generated_timestamp": datetime.utcnow().isoformat() + "Z",
            "generator": "ipmapper",
            "version": "1.1.0",
            "license": "MIT",
            "description": "IP-to-country mapping derived from RIR delegated files",
            "sources": download_metadata.get("sources", {}),
            "download_info": {
                "timestamp": download_metadata.get("download_timestamp"),
                "sources_count": len(download_metadata.get("sources", {})),
            },
            "files": files_info,
            "statistics": {
                "total_ipv4_aggregated": files_info.get(
                    "prefixes_ipv4_agg.csv", {}
                ).get("count", 0),
                "total_ipv6_aggregated": files_info.get(
                    "prefixes_ipv6_agg.csv", {}
                ).get("count", 0),
            },
            "conflicts": self._serialize_conflicts(conflicts),
            "usage_note": "This dataset is derived from public RIR delegated files and inherits their usage terms.",
        }

        metadata["note"] = "Only aggregated prefixes are stored for optimal performance"

        metadata_file = self.output_dir / "metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        files_info["metadata.json"] = {
            "path": str(metadata_file),
            "size": metadata_file.stat().st_size,
            "sha256": self._calculate_sha256(metadata_file),
            "count": 1,
        }

        print(f"  Metadata written")
        return metadata
