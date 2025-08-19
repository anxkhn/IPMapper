"""Command-line interface for ipmapper."""

import sys
import time
import click
from pathlib import Path

from .data_fetcher import DataFetcher
from .parser import RIRParser
from .aggregator import PrefixAggregator
from .output_writer import OutputWriter
from .lookup import IPLookup, lookup


@click.group()
@click.version_option(version="1.0")
def cli():
    """Fast offline IP-to-country lookup using RIR data."""
    pass


@cli.command()
@click.option("--force", is_flag=True, help="Force re-download even if data exists")
@click.option("--data-dir", type=click.Path(), help="Custom data directory")
def update(force, data_dir):
    """Download and process RIR data."""
    try:
        start_time = time.time()

        # Initialize components
        fetcher = DataFetcher(data_dir)
        parser = RIRParser()
        aggregator = PrefixAggregator()
        writer = OutputWriter(fetcher.processed_dir)

        # Download RIR data
        click.echo("Downloading RIR delegated files...")
        download_metadata = fetcher.download_rir_data(force=force)

        # Parse all files
        click.echo("\nParsing RIR files...")
        rir_files = fetcher.get_data_files()
        all_entries = parser.parse_all_files(rir_files)

        # Deduplicate
        deduplicated_entries, conflicts = parser.deduplicate_entries(all_entries)

        # Separate by IP version
        ipv4_entries, ipv6_entries = parser.separate_by_type(deduplicated_entries)

        # Aggregate for optimization
        click.echo("\nAggregating prefixes...")
        ipv4_agg = aggregator.aggregate_entries(ipv4_entries)
        ipv6_agg = aggregator.aggregate_entries(ipv6_entries)

        # Separate aggregated by type
        ipv4_agg_entries = [(p, cc) for p, cc in ipv4_agg if p.version == 4]
        ipv6_agg_entries = [(p, cc) for p, cc in ipv6_agg if p.version == 6]

        # Write output files (only aggregated for performance)
        click.echo("\nWriting output files...")
        files_info = writer.write_aggregated_csv_files(
            ipv4_agg_entries, ipv6_agg_entries
        )

        # Write metadata
        metadata = writer.write_metadata(download_metadata, files_info, conflicts)

        # Cleanup raw data to save space
        click.echo("Cleaning up raw data...")
        fetcher.cleanup_raw_data()

        elapsed = time.time() - start_time

        click.echo(f"\nUpdate completed in {elapsed:.1f}s")
        click.echo(f"Data directory: {fetcher.data_dir}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
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
def lookup(ips, output_format, country_name, currency, data_dir):
    """Look up country information for IP addresses."""
    try:
        # Initialize lookup
        if data_dir:
            lookup_engine = IPLookup(Path(data_dir) / "processed")
        else:
            lookup_engine = IPLookup()

        results = []
        for ip in ips:
            try:
                result = lookup_engine.lookup_full(ip)

                # Filter fields based on options
                filtered_result = {"ip": result["ip"]}
                filtered_result["country_code"] = result["country_code"]

                if country_name:
                    filtered_result["country_name"] = result["country_name"]
                if currency:
                    filtered_result["currency"] = result["currency"]

                results.append(filtered_result)

            except Exception as e:
                click.echo(f"Error looking up {ip}: {e}", err=True)
                continue

        # Output results
        if output_format == "json":
            import json

            click.echo(json.dumps(results, indent=2))
        elif output_format == "csv":
            if results:
                headers = list(results[0].keys())
                click.echo(",".join(headers))
                for result in results:
                    click.echo(",".join(str(result.get(h, "")) for h in headers))
        else:  # table
            if results:
                headers = list(results[0].keys())
                
                # Simple table formatting without tabulate
                col_widths = [max(len(h), max(len(str(result.get(h, ""))) for result in results)) for h in headers]
                
                # Header
                header_line = " | ".join(h.ljust(w) for h, w in zip(headers, col_widths))
                separator = "-+-".join("-" * w for w in col_widths)
                
                click.echo(header_line)
                click.echo(separator)
                
                # Rows
                for result in results:
                    row_line = " | ".join(str(result.get(h, "")).ljust(w) for h, w in zip(headers, col_widths))
                    click.echo(row_line)
            else:
                click.echo("No results found.")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--data-dir", type=click.Path(), help="Custom data directory")
def status(data_dir):
    """Show status of local data."""
    try:
        fetcher = DataFetcher(data_dir)

        click.echo("IPMap Status")
        click.echo("=" * 50)

        # Check data directory
        click.echo(f"Data directory: {fetcher.data_dir}")
        click.echo(f"Directory exists: {'Yes' if fetcher.data_dir.exists() else 'No'}")

        # Check processed data
        processed_dir = fetcher.processed_dir
        processed_files = [
            "prefixes_ipv4_agg.csv",
            "prefixes_ipv6_agg.csv",
            "metadata.json",
        ]

        click.echo(f"\nProcessed files:")
        for filename in processed_files:
            filepath = processed_dir / filename
            if filepath.exists():
                size = filepath.stat().st_size
                click.echo(f"  [OK] {filename}: {size:,} bytes")
            else:
                click.echo(f"  [MISSING] {filename}: missing")

        # Check metadata
        metadata = fetcher.get_metadata()
        if metadata:
            click.echo(
                f"\nLast update: {metadata.get('download_timestamp', 'Unknown')}"
            )
            stats = metadata.get("statistics", {})
        else:
            click.echo("\nNo metadata found.")

        # Recommendations
        click.echo("\nRecommendation: Run 'ipmapper update' to download/process data")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("ip")
def country(ip):
    """Get country name for an IP address."""
    try:
        from .lookup import get_country_name

        result = get_country_name(ip)
        click.echo(result or "Unknown")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command(name="country_code")
@click.argument("ip")
def country_code(ip):
    """Get country code for an IP address."""
    try:
        from .lookup import get_country_code

        result = get_country_code(ip)
        click.echo(result or "Unknown")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("ip")
def currency(ip):
    """Get currency for an IP address (shortcut)."""
    try:
        from .lookup import get_country_currency

        result = get_country_currency(ip)
        click.echo(result or "Unknown")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
