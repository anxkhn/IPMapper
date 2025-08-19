# IPMapper - Offline IP-to-Country Lookup

<p align="center">
  <strong>Fast, offline IP geolocation using Regional Internet Registry (RIR) data</strong><br>
  <em>No API calls • No rate limits • No signup required • Download once, use forever</em>
</p>

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

</div>

## Motivation

IPMapper addresses the need for **reliable, offline IP geolocation** without dependencies on external APIs or services. Built entirely on public Regional Internet Registry (RIR) data, it provides:

- **Payment processing**: Determine customer currency from IP for automatic payment localization
- **Feature restrictions**: Implement geo-blocking or feature availability based on country codes
- **Compliance**: Meet data residency requirements by identifying user locations
- **Analytics**: Understand user geographic distribution without third-party services
- **Offline environments**: Work in air-gapped systems or areas with limited internet connectivity

## Core Features

### Data Sources

- **100% Public Data**: Uses only official RIR delegated files from APNIC, ARIN, RIPE NCC, LACNIC, and AFRINIC
- **No External Dependencies**: Operates completely offline after initial data download
- **Always Free**: No API keys, accounts, or subscription fees required

### Geographic Information

- **Country Codes**: ISO 3166-1 alpha-2 country codes (US, DE, JP, etc.)
- **Country Names**: Full country names ("United States of America")
- **Currency Codes**: ISO 4217 currency codes (USD, EUR, JPY, etc.)
- **Dual Stack**: Full IPv4 and IPv6 support

### Technical Implementation

- **Radix Tree Lookup**: O(prefix length) complexity for sub-microsecond lookups
- **Prefix Aggregation**: 30-70% reduction in dataset size while maintaining accuracy
- **Memory Efficient**: Optimized data structures for minimal RAM usage
- **Auto-loading**: Data loads automatically on first use

## Technical Architecture

### Radix Tree Implementation

IPMapper uses a **radix tree** (compressed trie) for IP prefix lookups, providing optimal search complexity:

```
Time Complexity: O(k) where k = IP address bit length (32 for IPv4, 128 for IPv6)
Space Complexity: O(n×k) where n = number of unique prefixes
```

**Why Radix Trees?**

- **Longest Prefix Matching**: Automatically finds the most specific route for any IP
- **Memory Efficient**: Shared prefixes are stored only once
- **Cache Friendly**: Tree traversal exhibits good spatial locality
- **Predictable Performance**: Lookup time depends only on address length, not dataset size

### Prefix Aggregation

The library implements **CIDR prefix aggregation** to optimize storage and lookup performance:

**Aggregation Process:**

1. **Grouping**: Group prefixes by country code and IP version
2. **Sorting**: Sort prefixes by network address for efficient processing
3. **Merging**: Use `ipaddress.collapse_addresses()` to merge adjacent prefixes
4. **Validation**: Ensure aggregation preserves country code boundaries

**Benefits:**

- **Reduced Memory**: 30-70% fewer prefixes to store
- **Faster Loading**: Less data to process during initialization
- **Maintained Accuracy**: No loss of geographic precision
- **Better Cache Utilization**: Fewer tree nodes improve cache hit rates

## Installation

```bash
# Install from PyPI
pip install ipmapper

# Or install from source
git clone https://github.com/anxkhn/ipmapper
cd ipmapper
pip install -e .
```

## Quick Start

### First Run - Download Data

```bash
# Download and process RIR data (run once or when updating)
ipmapper update
```

### Command Line Usage

```bash
# Basic lookup
ipmapper lookup 8.8.8.8
ipmapper lookup 2001:4860:4860::8888

# Multiple IPs with additional data
ipmapper lookup 8.8.8.8 1.1.1.1 --country-name --currency

# Quick shortcuts
ipmapper country 8.8.8.8      # Just country name
ipmapper country_code 8.8.8.8  # Just country code
ipmapper currency 8.8.8.8     # Just currency

# Output formats
ipmapper lookup 8.8.8.8 --format json
ipmapper lookup 8.8.8.8 --format csv

# Check status
ipmapper status
```

### Python Library Usage

```python
import ipmapper

# Simple lookups (auto-loads data on first use)
result = ipmapper.lookup('8.8.8.8')
print(result)
# {'ip': '8.8.8.8', 'country_code': 'US', 'country_name': 'United States of America', 'currency': 'USD'}

# Direct attribute access
country_name = ipmapper.get_country_name('8.8.8.8')     # 'United States of America'
country_code = ipmapper.get_country_code('8.8.8.8')     # 'US'
currency = ipmapper.get_country_currency('8.8.8.8')     # 'USD'

# Selectively use IPv4 or IPv6 tree with IP version hints
result = ipmapper.lookup('192.168.1.1', ip_version='ipv4')  # Skip IPv6 tree
result = ipmapper.lookup('2001:db8::1', ip_version='ipv6')  # Skip IPv4 tree

# Advanced usage with custom data directory
lookup_engine = ipmapper.IPLookup(data_dir='/custom/path')
result = lookup_engine.lookup_full('8.8.8.8')

```

## Data Sources & Processing

### Regional Internet Registries

| RIR          | Region                          | Data Source                                                                                                   |
| ------------ | ------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| **APNIC**    | Asia-Pacific                    | [delegated-apnic-extended-latest](https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest)          |
| **ARIN**     | North America                   | [delegated-arin-extended-latest](https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest)          |
| **RIPE NCC** | Europe/Middle East/Central Asia | [delegated-ripencc-extended-latest](https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest) |
| **LACNIC**   | Latin America/Caribbean         | [delegated-lacnic-extended-latest](https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest)  |
| **AFRINIC**  | Africa                          | [delegated-afrinic-extended-latest](https://ftp.afrinic.net/stats/afrinic/delegated-afrinic-extended-latest)  |

### Processing Pipeline

1. **Download**: Fetch latest delegated files from all 5 RIRs
2. **Parse**: Extract IPv4/IPv6 allocations with country codes
   - Filter for `allocated` and `assigned` status only
   - Convert IPv4 address counts to CIDR blocks
   - Parse IPv6 prefix lengths directly
3. **Deduplicate**: Resolve overlapping entries
   - Prefer most recent allocation date
   - Use lexicographic registry ordering for ties
4. **Aggregate**: Optimize prefix lists using CIDR aggregation
5. **Generate**: Create optimized CSV files and metadata

## CLI Reference

### Core Commands

**`ipmapper update`** - Download and process RIR data

```bash
ipmapper update [OPTIONS]

Options:
  --force        Force re-download even if data exists
  --data-dir     Custom data directory (default: ~/.ipmapper)
```

**`ipmapper lookup`** - Look up IP addresses

```bash
ipmapper lookup [OPTIONS] IP1 [IP2 ...]

Options:
  --format [table|json|csv]  Output format (default: table)
  --country-name            Include country names
  --currency               Include currency codes
  --data-dir               Custom data directory
```

**`ipmapper status`** - Show local data status

```bash
ipmapper status [OPTIONS]

Options:
  --data-dir    Custom data directory
```

### Quick Commands

```bash
ipmapper country IP         # Get country name
ipmapper country_code IP    # Get country code
ipmapper currency IP        # Get currency code
```

## Output Files

After running `ipmapper update`, these files are generated:

- **`prefixes_ipv4_agg.csv`** - Aggregated IPv4 prefixes (format: `cidr,country_code`)
- **`prefixes_ipv6_agg.csv`** - Aggregated IPv6 prefixes (format: `cidr,country_code`)
- **`metadata.json`** - Source URLs, timestamps, checksums, and statistics

_Note: Only aggregated files are generated for optimal performance. Raw files are cleaned up automatically._

## Use Cases

### Payment Processing

```python
import ipmapper

def determine_currency(client_ip):
    """Automatically set payment currency based on client location"""
    currency = ipmapper.get_country_currency(client_ip)
    return currency or 'USD'  # Default to USD

# Usage
client_ip = request.remote_addr
payment_currency = determine_currency(client_ip)
```

### Feature Restrictions

```python
import ipmapper

RESTRICTED_COUNTRIES = {'CN', 'IR', 'KP'}  # Example restrictions

def check_feature_availability(client_ip, feature_name):
    """Check if feature is available in client's country"""
    country_code = ipmapper.get_country_code(client_ip)

    if country_code in RESTRICTED_COUNTRIES:
        return False
    return True
```

### Analytics & Compliance

```python
import ipmapper
from collections import Counter

def analyze_user_geography(ip_list):
    """Analyze geographic distribution of users"""
    countries = []
    for ip in ip_list:
        country = ipmapper.get_country_name(ip)
        if country:
            countries.append(country)

    return Counter(countries)
```

## Development

```bash
# Clone repository
git clone https://github.com/anxkhn/ipmapper
cd ipmapper

# Install in development mode
pip install -e .

# Run from source
python -m ipmapper update
python -m ipmapper lookup 8.8.8.8
```

## Why Always Free

This project will always remain free and open source because:

<table>
<tr>
<td><strong>Public Data Foundation</strong></td>
<td>Built entirely on publicly available RIR allocation data</td>
</tr>
<tr>
<td><strong>Educational Mission</strong></td>
<td>Helps people understand internet infrastructure and IP allocation</td>
</tr>
<tr>
<td><strong>Community Driven</strong></td>
<td>Created for developers, by developers, with no commercial agenda</td>
</tr>
<tr>
<td><strong>Transparency First</strong></td>
<td>All code, data sources, and methodologies are completely open</td>
</tr>
<tr>
<td><strong>Self-Contained</strong></td>
<td>No ongoing infrastructure costs or API maintenance requirements</td>
</tr>
</table>

## License

**MIT License** - see [LICENSE](LICENSE) file for details.

### Data License

The IP allocation data is derived from public RIR delegated files, available under their respective terms of use. This library inherits those terms for the data while the code remains MIT licensed.

## Related Projects

- **[GeoLite2](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data)** - MaxMind's free geolocation database
- **[ip2location-lite](https://github.com/ip2location/ip2location-python)** - Commercial IP geolocation with city-level data
- **[python-geoip](https://github.com/pierrrrrrre/python-geoip)** - Another Python IP geolocation library

## Support

- **Issues**: [GitHub Issues](https://github.com/anxkhn/ipmapper/issues)
- **Discussions**: [GitHub Discussions](https://github.com/anxkhn/ipmapper/discussions)
- **Email**: Create an issue for fastest response

---

<div align="center">
<strong>Built with ❤️ for the open internet community</strong>
</div>
