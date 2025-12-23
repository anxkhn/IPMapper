"""Fast IP lookup using radix trees."""

import csv
import ipaddress
import json
from pathlib import Path

from .countries import get_country_info


class RadixNode:
    """Node in a radix tree for IP prefix lookups."""

    def __init__(self):
        self.children = {}
        self.data = None
        self.is_terminal = False

    def insert(self, prefix_bits, country_code, depth=0):
        """Insert a prefix into the radix tree."""
        if depth == len(prefix_bits):
            self.data = country_code
            self.is_terminal = True
            return

        bit = prefix_bits[depth]
        if bit not in self.children:
            self.children[bit] = RadixNode()

        self.children[bit].insert(prefix_bits, country_code, depth + 1)

    def lookup(self, ip_bits, depth=0):
        """Look up an IP address in the radix tree."""
        best_match = self.data if self.is_terminal else None

        if depth >= len(ip_bits) or not self.children:
            return best_match

        bit = ip_bits[depth]
        if bit in self.children:
            child_result = self.children[bit].lookup(ip_bits, depth + 1)
            return child_result if child_result is not None else best_match

        return best_match


class IPLookup:
    """Fast IP-to-country lookup using radix trees."""

    def __init__(self, data_dir=None):
        """Initialize the IP lookup system.

        Args:
            data_dir: Directory containing processed data files
        """
        if data_dir is None:
            data_dir = Path.home() / ".ipmapper" / "processed"

        self.data_dir = Path(data_dir)
        self.ipv4_tree = RadixNode()
        self.ipv6_tree = RadixNode()
        self.metadata = {}
        self.loaded = False

    def _ip_to_bits(self, ip):
        """Convert IP address to binary string."""
        if isinstance(ip, str):
            try:
                ip = ipaddress.ip_address(ip)
            except ValueError as exc:
                raise ValueError(f"Invalid IP address: {ip}") from exc

        if isinstance(ip, ipaddress.IPv4Address):
            return format(int(ip), "032b")
        if isinstance(ip, ipaddress.IPv6Address):
            return format(int(ip), "0128b")
        raise ValueError(f"Unsupported IP type: {type(ip)}")

    def _prefix_to_bits(self, prefix):
        """Convert IP prefix to binary string of the network portion."""
        if isinstance(prefix, str):
            prefix = ipaddress.ip_network(prefix)

        network_int = int(prefix.network_address)
        if isinstance(prefix, ipaddress.IPv4Network):
            total_bits = 32
        else:
            total_bits = 128

        binary = format(network_int, f"0{total_bits}b")
        return binary[: prefix.prefixlen]

    def load_data(self):
        """Load processed data into radix trees."""
        ipv4_file = self.data_dir / "prefixes_ipv4_agg.csv"
        ipv6_file = self.data_dir / "prefixes_ipv6_agg.csv"
        metadata_file = self.data_dir / "metadata.json"

        if not ipv4_file.exists() or not ipv6_file.exists():
            print(
                f"Data files not found in {self.data_dir}. "
                "Run 'ipmapper update' to download and process data."
            )
            return False

        if metadata_file.exists():
            with open(metadata_file, encoding="utf-8") as f:
                self.metadata = json.load(f)

        self._load_ipv4_data(ipv4_file)
        self._load_ipv6_data(ipv6_file)

        self.loaded = True
        return True

    def _load_ipv4_data(self, ipv4_file):
        """Load IPv4 prefix data from CSV file."""
        with open(ipv4_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    prefix_str, country_code = row[0], row[1]
                    try:
                        prefix = ipaddress.IPv4Network(prefix_str)
                        prefix_bits = self._prefix_to_bits(prefix)
                        self.ipv4_tree.insert(prefix_bits, country_code.upper())
                    except (ValueError, TypeError) as e:
                        print(f"Warning: Failed to load IPv4 {prefix_str}: {e}")

    def _load_ipv6_data(self, ipv6_file):
        """Load IPv6 prefix data from CSV file."""
        with open(ipv6_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    prefix_str, country_code = row[0], row[1]
                    try:
                        prefix = ipaddress.IPv6Network(prefix_str)
                        prefix_bits = self._prefix_to_bits(prefix)
                        self.ipv6_tree.insert(prefix_bits, country_code.upper())
                    except (ValueError, TypeError) as e:
                        print(f"Warning: Failed to load IPv6 {prefix_str}: {e}")

    def lookup_ip(self, ip, ip_version=None):
        """Look up country code for an IP address."""
        if not self.loaded:
            if not self.load_data():
                return None

        try:
            ip_obj = ipaddress.ip_address(ip)
        except ValueError:
            print(f"Invalid IP address: {ip}")
            return None

        ip_bits = self._ip_to_bits(ip_obj)

        if ip_version == "ipv4" or (
            ip_version is None and isinstance(ip_obj, ipaddress.IPv4Address)
        ):
            return self.ipv4_tree.lookup(ip_bits)
        if ip_version == "ipv6" or (
            ip_version is None and isinstance(ip_obj, ipaddress.IPv6Address)
        ):
            return self.ipv6_tree.lookup(ip_bits)

        print(f"Invalid IP version hint: {ip_version}")
        return None

    def lookup_full(self, ip, ip_version=None):
        """Look up complete information for an IP address."""
        cc = self.lookup_ip(ip, ip_version)
        if cc:
            country_info = get_country_info(cc)
            return {
                "ip": str(ip),
                "country_code": cc,
                "country_name": country_info["name"],
                "currency": country_info["currency"],
            }
        return {
            "ip": str(ip),
            "country_code": None,
            "country_name": None,
            "currency": None,
        }


_GLOBAL_LOOKUP = None


def get_lookup():
    """Get or create global lookup instance."""
    global _GLOBAL_LOOKUP  # pylint: disable=global-statement
    if _GLOBAL_LOOKUP is None:
        _GLOBAL_LOOKUP = IPLookup()
    return _GLOBAL_LOOKUP


def lookup(ip, ip_version=None):
    """Main lookup function for both IPv4 and IPv6."""
    return get_lookup().lookup_full(ip, ip_version)


def get_country_name_for_ip(ip, ip_version=None):
    """Get country name for an IP address."""
    result = get_lookup().lookup_full(ip, ip_version)
    return result.get("country_name") if result else None


def get_country_code_for_ip(ip, ip_version=None):
    """Get country code for an IP address."""
    result = get_lookup().lookup_full(ip, ip_version)
    return result.get("country_code") if result else None


def get_country_currency_for_ip(ip, ip_version=None):
    """Get currency for an IP address."""
    result = get_lookup().lookup_full(ip, ip_version)
    return result.get("currency") if result else None


def ipv4_lookup(ip):
    """IPv4 lookup function."""
    return lookup(ip, "ipv4")


def ipv6_lookup(ip):
    """IPv6 lookup function."""
    return lookup(ip, "ipv6")
