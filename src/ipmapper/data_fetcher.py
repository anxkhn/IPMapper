"""Data fetcher for downloading RIR delegated files."""

import os
import json
import hashlib
import shutil
from datetime import datetime
from pathlib import Path
import requests
from tqdm import tqdm


# RIR sources
RIR_SOURCES = {
    "apnic": "https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest",
    "arin": "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
    "ripe": "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
    "lacnic": "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
    "afrinic": "https://ftp.afrinic.net/stats/afrinic/delegated-afrinic-extended-latest",
}


class DataFetcher:
    """Fetches RIR data files and manages caching."""

    def __init__(self, data_dir=None):
        """Initialize the data fetcher.

        Args:
            data_dir: Directory to store downloaded files. Defaults to ~/.ipmap
        """
        if data_dir is None:
            data_dir = Path.home() / ".ipmap"

        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.raw_dir = self.data_dir / "raw"
        self.raw_dir.mkdir(exist_ok=True)
        self.processed_dir = self.data_dir / "processed"
        self.processed_dir.mkdir(exist_ok=True)

    def _download_file(self, url, filepath, description=None):
        """Download a file with progress bar."""
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            desc = description or f"Downloading {filepath.name}"
            with (
                open(filepath, "wb") as f,
                tqdm(
                    desc=desc,
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as pbar,
            ):
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            return True

        except Exception as e:
            if filepath.exists():
                filepath.unlink()
            print(f"Failed to download {url}: {e}")
            return False

    def _calculate_sha256(self, filepath):
        """Calculate SHA256 hash of a file."""
        hash_sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def download_rir_data(self, force=False):
        """Download all RIR delegated files.

        Args:
            force: Force re-download even if files exist

        Returns:
            dict: Metadata about downloaded files
        """
        metadata = {
            "download_timestamp": datetime.utcnow().isoformat() + "Z",
            "sources": {},
            "files": {},
        }

        print("Downloading RIR delegated files...")

        for rir_name, url in RIR_SOURCES.items():
            print(f"\nDownloading {rir_name.upper()} data...")
            filepath = self.raw_dir / f"delegated-{rir_name}-extended-latest"

            if not force and filepath.exists():
                print(f"  {rir_name.upper()} data already exists, skipping...")
            else:
                if not self._download_file(
                    url, filepath, f"Downloading {rir_name.upper()}"
                ):
                    print(f"  Failed to download {rir_name.upper()}, skipping...")
                    continue

            # Calculate file info
            if not filepath.exists():
                continue
            file_size = filepath.stat().st_size
            file_hash = self._calculate_sha256(filepath)

            metadata["sources"][rir_name] = {
                "url": url,
                "file_path": str(filepath),
                "file_size": file_size,
                "sha256": file_hash,
            }

            print(
                f"  {rir_name.upper()}: {file_size:,} bytes (SHA256: {file_hash[:16]}...)"
            )

        # Save metadata
        metadata_file = self.data_dir / "download_metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        print(f"\nAll RIR data downloaded successfully")
        print(f"Data stored in: {self.data_dir}")

        return metadata

    def get_data_files(self):
        """Get paths to all downloaded RIR files."""
        files = {}
        for rir_name in RIR_SOURCES.keys():
            filepath = self.raw_dir / f"delegated-{rir_name}-extended-latest"
            if filepath.exists():
                files[rir_name] = filepath
        return files

    def is_data_available(self):
        """Check if all RIR data files are available."""
        files = self.get_data_files()
        return len(files) == len(RIR_SOURCES)

    def get_metadata(self):
        """Get download metadata if available."""
        metadata_file = self.data_dir / "download_metadata.json"
        if metadata_file.exists():
            with open(metadata_file) as f:
                return json.load(f)
        return None

    def cleanup_raw_data(self):
        """Remove raw data directory to save space."""
        if self.raw_dir.exists():
            try:
                shutil.rmtree(self.raw_dir)
                print(f"Cleaned up raw data directory: {self.raw_dir}")
            except Exception as e:
                print(f"Warning: Failed to cleanup raw data: {e}")
