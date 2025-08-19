"""
Fast offline IP-to-country lookup using RIR data.

This library provides IP-to-country mapping using data from Regional Internet Registries (RIRs).
It supports both IPv4 and IPv6 lookups with country names and currency information.
"""

__version__ = "1.0"

# Import main lookup functions for easy access
from .lookup import (
    lookup,
    get_country_name,
    get_country_code,
    get_country_currency,
    IPLookup,
    ipv4_lookup,
    ipv6_lookup,
)
from .countries import get_country_info

# Export main interface
__all__ = [
    "lookup",
    "get_country_name",
    "get_country_code",
    "get_country_currency",
    "get_country_info",
    "IPLookup",
    "ipv4_lookup",
    "ipv6_lookup",
]


def main():
    """Entry point for the CLI."""
    from .cli import cli

    cli()
