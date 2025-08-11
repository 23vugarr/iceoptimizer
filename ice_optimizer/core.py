# responsible for orchestration

from ice_optimizer.config_planner import ConfigExecutionPlanner
from ice_optimizer.config_file import ConfigFile
from ice_optimizer.sql_generator import SqlGenerator
from ice_optimizer.executor import PlanExecutor
from pyspark.sql import SparkSession
from ice_optimizer.parser_schema import IceoptimizerConfigSchema
from typing import Dict, Set

class Iceoptimizer:
    def __init__(
        self,
        root_folder: str = ".",
        max_concurrent_tasks: int = 10,
        spark: SparkSession = None,
    ):
        self.config_bag: IceoptimizerConfigSchema = ConfigFile(
            root_folder
        )()  # get all configurations from the root folder

        config_planner = ConfigExecutionPlanner(
            spark,
            configs=self.config_bag.configs,
            max_concurrent_tasks=max_concurrent_tasks,
            metadata_dir=self.config_bag.metadata_location,
        )  # extract tasks by urgency
        self.planned_configs, metadata_frame = config_planner.plan()

        self.sql_generator = SqlGenerator(self.planned_configs)
        self.optimization_sqls: Dict[str, Set[str]] = self.sql_generator.generate_sql()
        print(self.planned_configs)
        print(f"Generated SQLs: {self.optimization_sqls}")

        self.executor = PlanExecutor(spark, self.optimization_sqls, metadata_frame)

    def get_planned_configs(self) -> IceoptimizerConfigSchema:
        """
        Returns the planned configurations.
        """
        return self.planned_configs
    
    def get_generated_sqls(self) -> Dict[str, Set[str]]:
        """
        Returns the generated SQL statements for table optimization.
        """
        for table_hash, sqls in self.optimization_sqls.items():
            print(f"Table Hash: {table_hash}, SQLs: {sqls}")
        return self.optimization_sqls
    
    def run(self):
        """
        Runs the Iceoptimizer orchestration.
        This method can be called to execute the entire optimization process.
        """
        self.executor.execute()
        print("Iceoptimizer run completed.")
