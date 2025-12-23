"""Parser for RIR delegated files."""

import ipaddress
import warnings
from collections import defaultdict, namedtuple
from datetime import datetime

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
                max_block_size = 1
                while (
                    current % (max_block_size * 2) == 0
                    and current + max_block_size * 2 - 1 <= end_int
                ):
                    max_block_size *= 2

                prefix_len = 32 - (max_block_size - 1).bit_length()
                if max_block_size == 1:
                    prefix_len = 32

                cidr = ipaddress.IPv4Network(
                    f"{ipaddress.IPv4Address(current)}/{prefix_len}"
                )
                cidrs.append(cidr)
                current += max_block_size

            return cidrs

        except (ValueError, TypeError) as e:
            print(
                "Error converting IPv4 " + start_ip + "/" + str(count) + ": " + str(e)
            )
            return []

    def _parse_date(self, date_field):
        """Parse date field from RIR file."""
        try:
            if date_field and date_field.isdigit():
                return datetime.strptime(date_field, "%Y%m%d").date()
            return datetime(1900, 1, 1).date()
        except ValueError:
            return datetime(1900, 1, 1).date()

    def _parse_line(self, line, registry):
        """Parse a single line from RIR file."""
        line = line.strip()

        if not line or line.startswith("#"):
            return None

        parts = line.split("|")
        if len(parts) < 7:
            return None

        _, cc, type_field, start, value, date_field, status = parts[:7]

        if type_field not in self.valid_types or status not in self.valid_statuses:
            return None

        date = self._parse_date(date_field)

        prefixes = []
        if type_field == "ipv4":
            try:
                count = int(value)
                prefixes = self._ipv4_to_cidrs(start, count)
            except ValueError as e:
                warnings.warn(
                    "Failed to parse IPv4 " + start + "/" + value + ": " + str(e)
                )
                return None

        elif type_field == "ipv6":
            try:
                prefix_len = int(value)
                prefix = ipaddress.IPv6Network(f"{start}/{prefix_len}")
                prefixes = [prefix]
            except ValueError as e:
                warnings.warn(
                    "Failed to parse IPv6 " + start + "/" + value + ": " + str(e)
                )
                return None

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

        print("Parsing " + registry.upper() + " file...")

        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        parsed_entries = self._parse_line(line, registry)
                        if parsed_entries:
                            entries.extend(parsed_entries)
                    except ValueError as e:
                        warnings.warn(
                            "Error parsing line "
                            + str(line_num)
                            + " in "
                            + registry
                            + ": "
                            + str(e)
                        )
                        continue

        except OSError as e:
            print("Failed to parse file " + str(filepath) + ": " + str(e))
            return []

        print("  Parsed " + str(len(entries)) + " entries from " + registry.upper())
        return entries

    def parse_all_files(self, rir_files):
        """Parse all RIR files and return combined entries."""
        all_entries = []

        for registry, filepath in rir_files.items():
            entries = self.parse_file(filepath, registry)
            all_entries.extend(entries)

        print("\nTotal parsed entries: " + str(len(all_entries)))
        return all_entries

    def deduplicate_entries(self, entries):
        """Deduplicate overlapping entries."""
        print("Deduplicating entries...")

        prefix_groups = defaultdict(list)
        for entry in entries:
            prefix_groups[entry.prefix].append(entry)

        deduplicated = []
        conflicts = []

        for prefix, group in prefix_groups.items():
            if len(group) == 1:
                deduplicated.append(group[0])
            else:
                sorted_group = sorted(
                    group, key=lambda x: (x.date, x.registry), reverse=True
                )

                country_codes = {entry.cc for entry in group}
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
                "  Resolved "
                + str(len(conflicts))
                + " conflicts (chose most recent/lexicographically first)"
            )
            for conflict in conflicts[:5]:
                print(
                    "    "
                    + conflict["prefix"]
                    + ": "
                    + str(conflict["entries"])
                    + " -> "
                    + str(conflict["chosen"])
                )
            if len(conflicts) > 5:
                print("    ... and " + str(len(conflicts) - 5) + " more")

        print("  Deduplicated to " + str(len(deduplicated)) + " unique entries")
        return deduplicated, conflicts

    def separate_by_type(self, entries):
        """Separate entries by IPv4 and IPv6."""
        ipv4_entries = [e for e in entries if e.type == "ipv4"]
        ipv6_entries = [e for e in entries if e.type == "ipv6"]

        print("  IPv4 entries: " + str(len(ipv4_entries)))
        print("  IPv6 entries: " + str(len(ipv6_entries)))

        return ipv4_entries, ipv6_entries
