# responsible for executing sqls sent by planner

from pyspark.sql import SparkSession
from typing import List, Dict
from pyspark.sql import DataFrame


class PlanExecutor:
    def __init__(self, spark: SparkSession, sqls: Dict[str, List[str]], metadata_frame: DataFrame):
        self.spark = spark
        self.sqls = sqls
        self.metadata_frame = metadata_frame.select("config_file_name", 
                                       "catalog", 
                                       "schema_name", 
                                       "table_name", 
                                       "last_run_ts", 
                                       "run_interval_days", 
                                       "created_ts", 
                                       "max_run_date", 
                                       "hash")
        self.meta_file = "s3a://tmp/iceoptimizer/schedule_runs.parquet"

    def execute(self):
        for sql in self.sqls.values():
            for statement in sql:
                print(f"Executing SQL: {statement}")
                self.spark.sql(statement)
        self.metadata_frame.show()
        self.write_to_metadata()

    def write_to_metadata(self):
        """
        Writes the execution metadata to the specified directory.
        This is a placeholder method and should be implemented based on specific requirements.
        """
        # append to meta
        self.metadata_frame = self.metadata_frame.coalesce(1)
        self.metadata_frame.write.mode("append").parquet(self.meta_file)
        print(f"Metadata written to {self.meta_file}")
