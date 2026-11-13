# PyIceberg Drift Detector

A lightweight CLI tool to detect data drift between Apache Iceberg snapshots using PyIceberg and statistical tests (Kolmogorov-Smirnov).

## Why this tool?
Existing solutions for monitoring Iceberg tables often require heavy Spark clusters or specific cloud catalogs (like AWS Glue). This tool is designed for Data Engineers who want to quickly check for data drift locally or in CI/CD pipelines using pure Python, without the overhead.

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/pyiceberg-drift.git
cd pyiceberg-drift