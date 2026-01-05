"""Command-line interface for ipmapper."""

import json
import sys
import time
from pathlib import Path

import click

from .aggregator import PrefixAggregator
from .data_fetcher import DataFetcher
from .lookup import (
    IPLookup,
    get_country_code_for_ip,
    get_country_currency_for_ip,
    get_country_name_for_ip,
)
from .output_writer import OutputWriter
from .parser import RIRParser


@click.group()
@click.version_option(version="1.2.0")
def cli():
    """Fast offline IP-to-country lookup using RIR data."""


@cli.command()
@click.option("--force", is_flag=True, help="Force re-download even if data exists")
@click.option("--mmdb", is_flag=True, help="Generate MMDB binary database file")
@click.option("--data-dir", type=click.Path(), help="Custom data directory")
def update(force, mmdb, data_dir):
    """Download and process RIR data."""
    try:
        start_time = time.time()

        fetcher = DataFetcher(data_dir)
        parser = RIRParser()
        aggregator = PrefixAggregator()
        writer = OutputWriter(fetcher.processed_dir)

        download_metadata = fetcher.download_rir_data(force=force)

        click.echo("\nParsing RIR files...")
        rir_files = fetcher.get_data_files()
        all_entries = parser.parse_all_files(rir_files)

        deduplicated_entries, conflicts = parser.deduplicate_entries(all_entries)

        ipv4_entries, ipv6_entries = parser.separate_by_type(deduplicated_entries)

        click.echo("\nAggregating prefixes...")
        ipv4_agg = aggregator.aggregate_entries(ipv4_entries)
        ipv6_agg = aggregator.aggregate_entries(ipv6_entries)

        ipv4_agg_entries = [(p, cc) for p, cc in ipv4_agg if p.version == 4]
        ipv6_agg_entries = [(p, cc) for p, cc in ipv6_agg if p.version == 6]

        click.echo("\nWriting output files...")
        files_info = writer.write_aggregated_csv_files(
            ipv4_agg_entries, ipv6_agg_entries
        )

        if mmdb:
            mmdb_info = writer.write_mmdb_file(ipv4_agg_entries, ipv6_agg_entries)
            files_info.update(mmdb_info)

        writer.write_metadata(download_metadata, files_info, conflicts)

        click.echo("Cleaning up raw data...")
        fetcher.cleanup_raw_data()

        elapsed = time.time() - start_time

        click.echo(f"\nUpdate completed in {elapsed:.1f}s")
        click.echo(f"Data directory: {fetcher.data_dir}")

    except (OSError, ValueError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command(name="lookup")
@click.argument("ips", nargs=-1, required=True)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    help="Output format",
)
@click.option("--country-name", is_flag=True, help="Include country names")
@click.option("--currency", is_flag=True, help="Include currency codes")
@click.option("--data-dir", type=click.Path(), help="Custom data directory")
def lookup_cmd(ips, output_format, country_name, currency, data_dir):
    """Look up country information for IP addresses."""
    try:
        if data_dir:
            lookup_engine = IPLookup(Path(data_dir) / "processed")
        else:
            lookup_engine = IPLookup()

        results = []
        for ip in ips:
            try:
                result = lookup_engine.lookup_full(ip)
                filtered_result = {
                    "ip": result["ip"],
                    "country_code": result["country_code"],
                }
                if country_name:
                    filtered_result["country_name"] = result["country_name"]
                if currency:
                    filtered_result["currency"] = result["currency"]
                results.append(filtered_result)
            except ValueError as e:
                click.echo(f"Error looking up {ip}: {e}", err=True)
                continue

        _output_results(results, output_format)

    except (OSError, ValueError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def _output_results(results, output_format):
    """Output results in the specified format."""
    if output_format == "json":
        click.echo(json.dumps(results, indent=2))
    elif output_format == "csv":
        _output_csv(results)
    else:
        _output_table(results)


def _output_csv(results):
    """Output results in CSV format."""
    if results:
        headers = list(results[0].keys())
        click.echo(",".join(headers))
        for result in results:
            row = ",".join(str(result.get(h, "")) for h in headers)
            click.echo(row)


def _output_table(results):
    """Output results in table format."""
    if not results:
        click.echo("No results found.")
        return

    headers = list(results[0].keys())
    col_widths = []
    for h in headers:
        max_val_len = max(len(str(result.get(h, ""))) for result in results)
        col_widths.append(max(len(h), max_val_len))

    header_line = " | ".join(h.ljust(w) for h, w in zip(headers, col_widths))
    separator = "-+-".join("-" * w for w in col_widths)

    click.echo(header_line)
    click.echo(separator)

    for result in results:
        row_line = " | ".join(
            str(result.get(h, "")).ljust(w) for h, w in zip(headers, col_widths)
        )
        click.echo(row_line)


@cli.command()
@click.option("--data-dir", type=click.Path(), help="Custom data directory")
def status(data_dir):
    """Show status of local data."""
    try:
        fetcher = DataFetcher(data_dir)

        click.echo("IPMap Status")
        click.echo("=" * 50)

        click.echo(f"Data directory: {fetcher.data_dir}")
        exists_str = "Yes" if fetcher.data_dir.exists() else "No"
        click.echo(f"Directory exists: {exists_str}")

        processed_dir = fetcher.processed_dir
        processed_files = [
            "prefixes_ipv4_agg.csv",
            "prefixes_ipv6_agg.csv",
            "country.mmdb",
            "metadata.json",
        ]

        click.echo("\nProcessed files:")
        for filename in processed_files:
            filepath = processed_dir / filename
            if filepath.exists():
                size = filepath.stat().st_size
                click.echo(f"  [OK] {filename}: {size:,} bytes")
            else:
                click.echo(f"  [MISSING] {filename}: missing")

        metadata = fetcher.get_metadata()
        if metadata:
            timestamp = metadata.get("download_timestamp", "Unknown")
            click.echo(f"\nLast update: {timestamp}")
        else:
            click.echo("\nNo metadata found.")

        click.echo("\nRun 'ipmapper update' to download/process data")

    except (OSError, ValueError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("ip")
def country(ip):
    """Get country name for an IP address."""
    try:
        result = get_country_name_for_ip(ip)
        click.echo(result or "Unknown")
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command(name="country_code")
@click.argument("ip")
def country_code_cmd(ip):
    """Get country code for an IP address."""
    try:
        result = get_country_code_for_ip(ip)
        click.echo(result or "Unknown")
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command(name="currency")
@click.argument("ip")
def currency_cmd(ip):
    """Get currency for an IP address (shortcut)."""
    try:
        result = get_country_currency_for_ip(ip)
        click.echo(result or "Unknown")
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
