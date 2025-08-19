"""Parser for RIR delegated files."""

import re
import ipaddress
from datetime import datetime
from collections import defaultdict, namedtuple
import warnings


# Named tuple for parsed entries
RIREntry = namedtuple(
    "RIREntry", ["registry", "cc", "type", "start", "value", "date", "status", "prefix"]
)


class RIRParser:
    """Parser for RIR delegated extended files."""

    def __init__(self):
        self.valid_statuses = {"allocated", "assigned"}
        self.valid_types = {"ipv4", "ipv6"}

    def _ipv4_to_cidrs(self, start_ip, count):
        """Convert IPv4 start address and count to CIDR blocks."""
        try:
            start = ipaddress.IPv4Address(start_ip)
            start_int = int(start)
            end_int = start_int + count - 1

            cidrs = []
            current = start_int

            while current <= end_int:
                # Find the largest block that fits
                max_block_size = 1
                while (
                    current % (max_block_size * 2) == 0
                    and current + max_block_size * 2 - 1 <= end_int
                ):
                    max_block_size *= 2

                # Convert to CIDR
                prefix_len = 32 - (max_block_size - 1).bit_length()
                if max_block_size == 1:
                    prefix_len = 32

                cidr = ipaddress.IPv4Network(
                    f"{ipaddress.IPv4Address(current)}/{prefix_len}"
                )
                cidrs.append(cidr)
                current += max_block_size

            return cidrs

        except Exception as e:
            print(f"Error converting IPv4 {start_ip}/{count}: {e}")
            return []

    def _parse_line(self, line, registry):
        """Parse a single line from RIR file."""
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith("#"):
            return None

        parts = line.split("|")
        if len(parts) < 7:
            return None

        registry_field, cc, type_field, start, value, date_field, status = parts[:7]

        # Filter by type and status
        if type_field not in self.valid_types or status not in self.valid_statuses:
            return None

        # Parse date
        try:
            if date_field and date_field.isdigit():
                date = datetime.strptime(date_field, "%Y%m%d").date()
            else:
                date = datetime(1900, 1, 1).date()  # Default for missing dates
        except:
            date = datetime(1900, 1, 1).date()

        # Convert to network prefixes
        prefixes = []
        if type_field == "ipv4":
            try:
                count = int(value)
                prefixes = self._ipv4_to_cidrs(start, count)
            except Exception as e:
                warnings.warn(f"Failed to parse IPv4 {start}/{value}: {e}")
                return None

        elif type_field == "ipv6":
            try:
                prefix_len = int(value)
                prefix = ipaddress.IPv6Network(f"{start}/{prefix_len}")
                prefixes = [prefix]
            except Exception as e:
                warnings.warn(f"Failed to parse IPv6 {start}/{value}: {e}")
                return None

        # Create entries for each prefix
        entries = []
        for prefix in prefixes:
            entry = RIREntry(
                registry=registry,
                cc=cc.upper(),
                type=type_field,
                start=start,
                value=value,
                date=date,
                status=status,
                prefix=prefix,
            )
            entries.append(entry)

        return entries

    def parse_file(self, filepath, registry):
        """Parse an RIR delegated file."""
        entries = []

        print(f"Parsing {registry.upper()} file...")

        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        parsed_entries = self._parse_line(line, registry)
                        if parsed_entries:
                            entries.extend(parsed_entries)
                    except Exception as e:
                        warnings.warn(
                            f"Error parsing line {line_num} in {registry}: {e}"
                        )
                        continue

        except Exception as e:
            print(f"Failed to parse file {filepath}: {e}")
            return []

        print(f"  Parsed {len(entries)} entries from {registry.upper()}")
        return entries

    def parse_all_files(self, rir_files):
        """Parse all RIR files and return combined entries."""
        all_entries = []

        for registry, filepath in rir_files.items():
            entries = self.parse_file(filepath, registry)
            all_entries.extend(entries)

        print(f"\nTotal parsed entries: {len(all_entries)}")
        return all_entries

    def deduplicate_entries(self, entries):
        """Deduplicate overlapping entries."""
        print("Deduplicating entries...")

        # Group by prefix for deduplication
        prefix_groups = defaultdict(list)
        for entry in entries:
            prefix_groups[entry.prefix].append(entry)

        deduplicated = []
        conflicts = []

        for prefix, group in prefix_groups.items():
            if len(group) == 1:
                deduplicated.append(group[0])
            else:
                # Multiple entries for same prefix - resolve conflict
                # Sort by date (newest first), then by registry name for determinism
                sorted_group = sorted(
                    group, key=lambda x: (x.date, x.registry), reverse=True
                )

                # Check if country codes differ
                country_codes = set(entry.cc for entry in group)
                if len(country_codes) > 1:
                    conflicts.append(
                        {
                            "prefix": str(prefix),
                            "entries": [(e.registry, e.cc, e.date) for e in group],
                            "chosen": (
                                sorted_group[0].registry,
                                sorted_group[0].cc,
                                sorted_group[0].date,
                            ),
                        }
                    )

                deduplicated.append(sorted_group[0])

        if conflicts:
            print(
                f"  Resolved {len(conflicts)} conflicts (chose most recent/lexicographically first)"
            )
            for conflict in conflicts[:5]:  # Show first 5 conflicts
                print(
                    f"    {conflict['prefix']}: {conflict['entries']} -> {conflict['chosen']}"
                )
            if len(conflicts) > 5:
                print(f"    ... and {len(conflicts) - 5} more")

        print(f"  Deduplicated to {len(deduplicated)} unique entries")
        return deduplicated, conflicts

    def separate_by_type(self, entries):
        """Separate entries by IPv4 and IPv6."""
        ipv4_entries = [e for e in entries if e.type == "ipv4"]
        ipv6_entries = [e for e in entries if e.type == "ipv6"]

        print(f"  IPv4 entries: {len(ipv4_entries)}")
        print(f"  IPv6 entries: {len(ipv6_entries)}")

        return ipv4_entries, ipv6_entries
