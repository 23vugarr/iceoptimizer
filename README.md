# IceOptimizer – Modular Apache Iceberg Table Optimization

## Overview
**IceOptimizer** is a lightweight and modular framework that simplifies optimization of **Apache Iceberg** tables.  
It allows you to define **optimization tasks** for multiple catalogs, schemas, and tables in a **single configuration file**, with fine-grained control over the optimization strategies and schedules.

The goal is to make table maintenance **easy, configurable, and automatable** without having to manually run Spark or Trino commands for every table.

---

## Features
- **Modular Configurations** – Configure multiple schemas and tables in a single JSON file.
- **Flexible Optimization Strategies** – Support for:
  - Data file rewriting
  - Manifest file rewriting
  - Position delete file rewriting
- **Custom Run Intervals** – Define `run_interval_days` for each table or table group.
- **Auto Optimization** – Enable simple `auto_optimize` mode for quick setup.
- **Partial Table Matching** – Apply the same optimization config to multiple tables via `table_list`.
- **S3 & Lakehouse Ready** – Supports Iceberg catalogs stored in S3.

---

## Example Configuration

```json
{
    "catalog": "lakehouse",
    "metadata_location": "s3a://tmp/iceoptimizer",
    "schemas": [
        {
            "schema_name": "sales",
            "tables": [
                {
                    "table_name": "fct_smth_sales",
                    "auto_optimize": true,
                    "run_interval_days": 3
                },
                {
                    "table_name": "dim_users",
                    "rewrite_data_files": {
                        "write.update.mode": "copy-on-read"
                    },
                    "rewrite_manifest_files": {
                        "partial.delete": "true"
                    },
                    "rewrite_position_delete_files": {
                        "options.example.conf": "copy-on-read"
                    },
                    "run_interval_days": 7
                }
            ]
        },
        {
            "schema_name": "corporate",
            "tables": [
                {
                    "table_list": ["example_table_1", "example_table_2"],
                    "rewrite_manifest_files": {
                        "partial.delete": "true",
                        "example.rewrite": "5",
                        "options.example.conf": "copy-on-read"
                    },
                    "run_interval_days": 5
                }
            ]
        },
        {
            "schema_name": "finance",
            "tables": [
                {
                    "table_list": ["example_table_finance", "example_table_finance"],
                    "rewrite_data_files": {
                        "partial.delete": "true",
                        "example.rewrite": "5",
                        "options.example.conf": "copy-on-read"
                    },
                    "run_interval_days": 5
                }
            ]
        }
    ]
}
```

## How It Works
1. The configuration file is loaded by the optimization runner.
2. Each table’s last run date is checked against run_interval_days. (only urgent ones are run in accordance with __max_concurrent_tasks__)
3. Only due tables are optimized in the current run.
4. Optimizations are executed in Spark using the provided rewrite settings.
5. The schedule state is persisted to avoid duplicate runs.

## License

This project is licensed under the **MIT License**.
