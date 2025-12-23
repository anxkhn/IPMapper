"""Data fetcher for downloading RIR delegated files."""

import os
import json
import hashlib
import shutil
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from tqdm import tqdm


RIR_SOURCES = {
    "apnic": [
        "https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest",
        "https://ftp.ripe.net/pub/stats/apnic/delegated-apnic-extended-latest",
    ],
    "arin": [
        "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
        "https://ftp.ripe.net/pub/stats/arin/delegated-arin-extended-latest",
        "https://ftp.apnic.net/stats/arin/delegated-arin-extended-latest",
    ],
    "ripe": [
        "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
        "https://ftp.apnic.net/stats/ripe-ncc/delegated-ripencc-extended-latest",
        "https://ftp.lacnic.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
    ],
    "lacnic": [
        "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
        "https://ftp.ripe.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
    ],
    "afrinic": [
        "https://ftp.afrinic.net/stats/afrinic/delegated-afrinic-extended-latest",
        "https://ftp.ripe.net/pub/stats/afrinic/delegated-afrinic-extended-latest",
        "https://ftp.apnic.net/stats/afrinic/delegated-afrinic-extended-latest",
    ],
}


class DataFetcher:
    """Fetches RIR data files and manages caching."""

    MAX_RETRIES = 3
    RETRY_BACKOFF = 1.0
    TIMEOUT = (10, 60)
    MAX_WORKERS = 5
    CHUNK_SIZE = 8192

    def __init__(self, data_dir=None):
        if data_dir is None:
            data_dir = Path.home() / ".ipmapper"

        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.raw_dir = self.data_dir / "raw"
        self.raw_dir.mkdir(exist_ok=True)
        self.processed_dir = self.data_dir / "processed"
        self.processed_dir.mkdir(exist_ok=True)

    def _create_session(self):
        session = requests.Session()
        retry = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=self.RETRY_BACKOFF,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _download_file(self, session, url, filepath, description=None, resume=True):
        """Download a file with progress bar, retry, and resume support."""
        resume_header = {}
        mode = "wb"
        existing_size = 0

        if resume and filepath.exists():
            existing_size = filepath.stat().st_size
            resume_header = {"Range": f"bytes={existing_size}-"}
            mode = "ab"

        try:
            response = session.get(
                url, stream=True, headers=resume_header, timeout=self.TIMEOUT
            )

            if response.status_code == 416:
                return True

            if response.status_code == 206:
                total_size = existing_size + int(
                    response.headers.get("content-length", 0)
                )
            elif response.status_code == 200:
                existing_size = 0
                mode = "wb"
                total_size = int(response.headers.get("content-length", 0))
            else:
                response.raise_for_status()
                total_size = 0

            desc = description or f"Downloading {filepath.name}"
            with (
                open(filepath, mode) as f,
                tqdm(
                    desc=desc,
                    total=total_size,
                    initial=existing_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as pbar,
            ):
                for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            return True

        except requests.exceptions.RequestException as e:
            if filepath.exists() and mode == "wb":
                filepath.unlink()
            raise e

    def _download_single(self, session, rir_name, urls, filepath, force=False):
        """Download a single RIR file with fallback support."""
        if not force and filepath.exists():
            return {
                "rir": rir_name,
                "status": "skipped",
                "message": f"{rir_name.upper()} data already exists",
            }

        last_error = None
        for try_url in urls:
            try:
                self._download_file(
                    session, try_url, filepath, f"Downloading {rir_name.upper()}"
                )
                return {"rir": rir_name, "status": "success", "url": try_url}
            except Exception as e:
                last_error = e
                if filepath.exists():
                    filepath.unlink()
                continue

        return {
            "rir": rir_name,
            "status": "failed",
            "error": str(last_error),
        }

    def _calculate_sha256(self, filepath):
        """Calculate SHA256 hash of a file."""
        hash_sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def download_rir_data(self, force=False):
        """Download all RIR delegated files in parallel.

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

        session = self._create_session()
        results = {}

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = {}
            for rir_name, urls in RIR_SOURCES.items():
                filepath = self.raw_dir / f"delegated-{rir_name}-extended-latest"
                future = executor.submit(
                    self._download_single, session, rir_name, urls, filepath, force
                )
                futures[future] = rir_name

            for future in as_completed(futures):
                rir_name = futures[future]
                try:
                    result = future.result()
                    results[rir_name] = result

                    if result["status"] == "skipped":
                        print(f"\n{result['message']}, skipping...")
                    elif result["status"] == "failed":
                        print(f"\nFailed to download {rir_name.upper()}, skipping...")
                except Exception as e:
                    print(f"\nError downloading {rir_name.upper()}: {e}")
                    results[rir_name] = {"rir": rir_name, "status": "error", "error": str(e)}

        for rir_name in RIR_SOURCES:
            filepath = self.raw_dir / f"delegated-{rir_name}-extended-latest"
            if not filepath.exists():
                continue

            file_size = filepath.stat().st_size
            file_hash = self._calculate_sha256(filepath)

            metadata["sources"][rir_name] = {
                "url": RIR_SOURCES[rir_name][0],
                "file_path": str(filepath),
                "file_size": file_size,
                "sha256": file_hash,
            }

            print(
                f"  {rir_name.upper()}: {file_size:,} bytes (SHA256: {file_hash[:16]}...)"
            )

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
