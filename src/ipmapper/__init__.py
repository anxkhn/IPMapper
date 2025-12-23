"""
Fast offline IP-to-country lookup using RIR data.

This library provides IP-to-country mapping using data from
Regional Internet Registries (RIRs). It supports both IPv4 and
IPv6 lookups with country names and currency information.
"""

__version__ = "1.1.1"

from .countries import get_country_info
from .lookup import (
    IPLookup,
    get_country_code_for_ip,
    get_country_currency_for_ip,
    get_country_name_for_ip,
    ipv4_lookup,
    ipv6_lookup,
    lookup,
)

__all__ = [
    "lookup",
    "get_country_name_for_ip",
    "get_country_code_for_ip",
    "get_country_currency_for_ip",
    "get_country_info",
    "IPLookup",
    "ipv4_lookup",
    "ipv6_lookup",
]


def main():
    """Entry point for the CLI."""
    from .cli import cli  # pylint: disable=import-outside-toplevel

    cli()
