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

        # Group by country code and IP version
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
                        f"  Processing group {processed}/{total_groups} ({cc} {ip_version})"
                    )

                # Use ipaddress.collapse_addresses for efficient aggregation
                try:
                    collapsed = list(ipaddress.collapse_addresses(prefixes))
                    for prefix in collapsed:
                        aggregated_pairs.append((prefix, cc))
                except Exception as e:
                    # Fallback to original prefixes if aggregation fails
                    print(f"  Warning: Aggregation failed for {cc} {ip_version}: {e}")
                    for prefix in prefixes:
                        aggregated_pairs.append((prefix, cc))

        # Sort final result
        aggregated_pairs.sort(key=lambda x: (str(type(x[0])), x[0].network_address))

        reduction = (
            100 * (1 - len(aggregated_pairs) / original_count)
            if original_count > 0
            else 0
        )
        print(
            f"  Aggregated {original_count:,} -> {len(aggregated_pairs):,} prefixes "
            f"({reduction:.1f}% reduction)"
        )

        return aggregated_pairs

    def aggregate_entries(self, entries):
        """Aggregate RIR entries by converting to prefix-CC pairs first."""
        # Convert entries to (prefix, cc) pairs
        prefix_cc_pairs = [(entry.prefix, entry.cc) for entry in entries]

        # Aggregate
        aggregated_pairs = self.aggregate_prefixes(prefix_cc_pairs)

        return aggregated_pairs
