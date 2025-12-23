"""Prefix aggregation for optimizing IP prefix lists."""

import ipaddress
from collections import defaultdict


class PrefixAggregator:
    """Aggregates IP prefixes to minimize the number of entries."""

    def aggregate_prefixes(self, prefix_cc_pairs):
        """Aggregate prefixes while preserving country code grouping.

        Args:
            prefix_cc_pairs: List of (prefix, country_code) tuples

        Returns:
            List of aggregated (prefix, country_code) tuples
        """
        print("Aggregating prefixes...")

        groups = defaultdict(lambda: defaultdict(list))

        for prefix, cc in prefix_cc_pairs:
            ip_version = "ipv4" if isinstance(prefix, ipaddress.IPv4Network) else "ipv6"
            groups[cc][ip_version].append(prefix)

        aggregated_pairs = []
        original_count = len(prefix_cc_pairs)
        processed = 0
        total_groups = sum(len(version_groups) for version_groups in groups.values())

        for cc, version_groups in groups.items():
            for ip_version, prefixes in version_groups.items():
                processed += 1
                if processed % 10 == 0 or processed == total_groups:
                    print(
                        "  Processing group "
                        + str(processed)
                        + "/"
                        + str(total_groups)
                        + " ("
                        + cc
                        + " "
                        + ip_version
                        + ")"
                    )

                try:
                    collapsed = list(ipaddress.collapse_addresses(prefixes))
                    for prefix in collapsed:
                        aggregated_pairs.append((prefix, cc))
                except (ValueError, TypeError) as e:
                    print(
                        "  Warning: Aggregation failed for "
                        + cc
                        + " "
                        + ip_version
                        + ": "
                        + str(e)
                    )
                    for prefix in prefixes:
                        aggregated_pairs.append((prefix, cc))

        aggregated_pairs.sort(key=lambda x: (str(type(x[0])), x[0].network_address))

        reduction = (
            100 * (1 - len(aggregated_pairs) / original_count)
            if original_count > 0
            else 0
        )
        print(
            "  Aggregated "
            + str(original_count)
            + " -> "
            + str(len(aggregated_pairs))
            + " prefixes ("
            + f"{reduction:.1f}"
            + "% reduction)"
        )

        return aggregated_pairs

    def aggregate_entries(self, entries):
        """Aggregate RIR entries by converting to prefix-CC pairs first."""
        prefix_cc_pairs = [(entry.prefix, entry.cc) for entry in entries]
        return self.aggregate_prefixes(prefix_cc_pairs)
