"""Prefix aggregation for optimizing IP prefix lists."""

import ipaddress
from collections import defaultdict


class PrefixAggregator:
    """Aggregates IP prefixes to minimize the number of entries."""

    def __init__(self):
        pass

    def _can_aggregate(self, net1, net2):
        """Check if two networks can be aggregated."""
        try:
            # Networks must be the same type (IPv4 or IPv6)
            if type(net1) != type(net2):
                return False

            # Networks must have the same prefix length
            if net1.prefixlen != net2.prefixlen:
                return False

            # Networks must be adjacent
            if net1.prefixlen == 0:
                return False

            # Calculate the supernet
            parent_prefixlen = net1.prefixlen - 1
            net1_parent = net1.supernet(new_prefix=parent_prefixlen)
            net2_parent = net2.supernet(new_prefix=parent_prefixlen)

            # They can aggregate if they have the same parent and cover the entire parent
            if net1_parent == net2_parent:
                # Check if they're the two halves of the parent network
                subnets = list(net1_parent.subnets(new_prefix=net1.prefixlen))
                return set([net1, net2]) == set(subnets)

            return False

        except:
            return False

    def _aggregate_pair(self, net1, net2):
        """Aggregate two networks into their supernet."""
        if not self._can_aggregate(net1, net2):
            return None

        parent_prefixlen = net1.prefixlen - 1
        return net1.supernet(new_prefix=parent_prefixlen)

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
