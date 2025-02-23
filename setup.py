from setuptools import setup, find_packages
import os

# Read README.md if it exists, otherwise use a basic description
try:
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = "A utility to synchronize historical candle data from Bitfinex"

setup(
    name="candles_sync",
    version="0.1.0",
    author="Bwahharharrr",
    author_email="your.email@example.com",
    description="A utility to synchronize historical candle data from Bitfinex",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Bwahharharrr/candles-sync",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Office/Business :: Financial :: Investment",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.32.3",
        "pandas>=2.2.3",
        "colorama>=0.4.6",
    ],
    entry_points={
        'console_scripts': [
            'candles-sync=candles_sync:main',
        ],
    },
)