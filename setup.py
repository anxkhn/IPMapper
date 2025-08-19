#!/usr/bin/env python3

import setuptools
from pathlib import Path

# Read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

setuptools.setup(
    name="ipmapper",
    version="1.0.0",
    author="Anas Khan",
    author_email="anxkhn28@gmail.com",
    description="Fast offline IP-to-country lookup using RIR data with country names and currency support",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/anxkhn/ipmapper",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Networking",
    ],
    python_requires=">=3.11",
    install_requires=[
        "requests>=2.31.0",
        "click>=8.1.0",
        "tqdm>=4.65.0",
    ],
    entry_points={
        "console_scripts": [
            "ipmapper=ipmapper:main",
        ],
    },
    keywords=["ip", "geolocation", "country", "currency", "rir", "offline", "radix-tree"],
    project_urls={
        "Homepage": "https://github.com/anxkhn/ipmapper",
        "Repository": "https://github.com/anxkhn/ipmapper",
        "Issues": "https://github.com/anxkhn/ipmapper/issues",
    },
)
