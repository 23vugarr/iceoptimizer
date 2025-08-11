# responsible for plannig execution of configs - not all configs can be executed at once

from pyspark.sql import SparkSession, DataFrame
from typing import List
from datetime import datetime
from ice_optimizer.parser_schema import IndividualTableConfig
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, when, col, current_date, datediff, when
from ice_optimizer.parser_schema import IceoptimizerConfigSchema
from typing import Tuple


class ConfigExecutionPlanner:
    def __init__(
        self,
        spark: SparkSession = None,
        metadata_dir: str = "s3a://tmp/iceoptimizer",
        configs: List[IndividualTableConfig] = None,
        max_concurrent_tasks: int = 10,
    ):
        if spark is None:
            raise ValueError("Spark session is required for ConfigPlanner.")

        self.spark = spark
        self.configs: List[IndividualTableConfig] = configs
        self.max_concurrent_tasks = max_concurrent_tasks
        self.metadata_dir = metadata_dir
        meta_file = f"{metadata_dir}/schedule_runs.parquet"
        try:
            self.schedule_runs: DataFrame = self.spark.read.parquet(meta_file)
            self.schedule_runs = self._get_latest_records_by_hash()
        except Exception:
            self.schedule_runs = spark.createDataFrame(
                [],
                schema="""
                    config_file_name STRING,
                    catalog STRING,
                    schema_name STRING,
                    table_name STRING,
                    last_run_ts TIMESTAMP,
                    run_interval_days INT,
                    created_ts TIMESTAMP,
                    max_run_date TIMESTAMP,
                    hash STRING
                """,
            )

    def _get_latest_records_by_hash(self) -> DataFrame:
        window = Window.partitionBy("hash").orderBy(col("last_run_ts").desc())
        return self.schedule_runs.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")

    def _current_time(self):
        return datetime.now()

    def plan(self) -> Tuple[IceoptimizerConfigSchema, DataFrame]:
        """
        Plans the execution of configurations based on the current schedule.
        """
        now = self._current_time()

        configs_df = self.spark.createDataFrame(
            [
                (
                    cfg.base_config_name,
                    cfg.catalog,
                    cfg.schema_name,
                    cfg.table_name,
                    cfg.run_interval_days,
                )
                for cfg in self.configs
            ],
            [
                "config_file_name",
                "catalog",
                "schema_name",
                "table_name",
                "run_interval_days",
            ],
        )

        configs_df = configs_df.withColumn(
            "hash",
            F.md5(
                F.concat_ws(
                    ":",
                    "config_file_name",
                    "catalog",
                    "schema_name",
                    "table_name",
                    "run_interval_days",
                )
            ),
        )
        merged_df_by_hash = configs_df.join(
            self.schedule_runs, on="hash", how="left"
        ).select(
            configs_df["*"],
            self.schedule_runs["last_run_ts"],
            self.schedule_runs["created_ts"],
            self.schedule_runs["max_run_date"],
        )

        merged_df_by_hash = merged_df_by_hash.withColumn(
            "created_ts",
            F.when(col("created_ts").isNull(), F.lit(now)).otherwise(col("created_ts")),
        )

        merged_df_by_hash = merged_df_by_hash.withColumn(
            "updated_max_run_date",
            when(
                col("max_run_date").isNull(),
                expr("created_ts + make_interval(0, 0, 0, run_interval_days)"),
            ).otherwise(
                when(
                    col("max_run_date") < now,
                    expr("max_run_date + make_interval(0, 0, 0, run_interval_days)"),
                ).otherwise(col("max_run_date"))
            ),
        )

        merged_df_by_hash = merged_df_by_hash.withColumn(
            "should_run", (col("updated_max_run_date") != col("max_run_date"))
        )

        merged_df_by_hash = merged_df_by_hash.withColumn(
            "max_run_date", col("updated_max_run_date")
        ).drop("updated_max_run_date")

        merged_df_by_hash = merged_df_by_hash.withColumn(
            "should_run",
            when(col("last_run_ts").isNull(), True).otherwise(col("should_run")),
        )

        merged_df_by_hash = merged_df_by_hash.filter(
            col("should_run")
        ).drop("should_run")

        merged_df_by_hash = merged_df_by_hash.withColumn(
            "days_until_due", datediff(col("max_run_date"), current_date())
        )

        merged_df_by_hash = (
            merged_df_by_hash.withColumn(
                "urgency_score",
                when(col("days_until_due") < 0, 1000).otherwise(
                    1 / (1 + col("days_until_due").cast("double"))
                ),
            )
            .orderBy(col("urgency_score").desc())
            .limit(self.max_concurrent_tasks)
        )

        merged_df_by_hash = merged_df_by_hash.withColumn(
            "last_run_ts",
            F.lit(now)
        )

        merged_df_by_hash.show()

        meta_confs = [
            (
                row.config_file_name,
                row.catalog,
                row.schema_name,
                row.table_name,
                row.run_interval_days,
            )
            for row in merged_df_by_hash.collect()
        ]

        planned_configs = IceoptimizerConfigSchema(
            metadata_location=self.metadata_dir,
            configs=[
                cfg
                for cfg in self.configs
                if (
                    cfg.base_config_name,
                    cfg.catalog,
                    cfg.schema_name,
                    cfg.table_name,
                    cfg.run_interval_days,
                )
                in meta_confs
            ]
        )

        return planned_configs, merged_df_by_hash
