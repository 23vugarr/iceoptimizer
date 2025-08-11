# responsible for individual configuration
# it finds the configuration files and returns internal representation of the configuration

from ice_optimizer.parser_schema import (
    IceoptimizerConfigSchema,
    BaseConfig,
    IndividualTableConfig,
)
from typing import List
import os


class ConfigFile:
    def __init__(self, root_folder: str = "."):
        self.configs: List[str] = self._collect_configs(
            root_folder
        )  # list of configuration file paths
        self.config_bag: List[BaseConfig] = (
            self._add_to_config_bag()
        )  # list of BaseConfig instances
        self.internal_representation: IceoptimizerConfigSchema = (
            self._add_to_config_bag_from_schema()
        )

    def __call__(self) -> IceoptimizerConfigSchema:
        """
        Returns the optimized configuration bag.
        """
        return self.internal_representation

    def _collect_configs(self, root_folder: str = "."):
        """
        Collects all configuration files from the specified root folder.
        Config file names must start with 'iceoptimizer_' and end with '.json'.
        """
        configs: List[str] = []
        for root, _, files in os.walk(root_folder):
            for file in files:
                if file.startswith("iceoptimizer_") and file.endswith(".json"):
                    configs.append(os.path.join(root, file))
        if not configs:
            raise ValueError(
                f"No configuration files found in {root_folder}. Please ensure files start with 'iceoptimizer_' and end with '.json'."
            )

        return configs

    def _add_to_config_bag(self):
        """
        Merges all configuration files into a single configuration bag.
        Each configuration file is validated against the BaseConfig model.
        Returns a list of BaseConfig instances.
        """
        config_bag: list[BaseConfig] = []

        for config_file in self.configs:
            with open(config_file, "r") as file:
                config_data = file.read()
                config_model = BaseConfig.model_validate_json(config_data)
                config_model.base_config_name = (
                    config_file.split("/")[-1]
                    .replace("iceoptimizer_", "")
                    .replace(".json", "")
                )
                config_bag.append(config_model)

        return config_bag

    def _add_to_config_bag_from_schema(self):
        """
        Adds configurations from the internal representation schema to the config bag.
        """
        internal_representation = IceoptimizerConfigSchema(metadata_location="", configs=[])

        for config in self.config_bag:
            for schema in config.schemas:
                for table in schema.tables:
                    if table.table_name:
                        individual_config = IndividualTableConfig(
                            base_config_name=config.base_config_name,
                            catalog=config.catalog,
                            schema_name=schema.schema_name,
                            table_name=table.table_name,
                            auto_optimize=table.auto_optimize,
                            rewrite_data_files=table.rewrite_data_files,
                            rewrite_manifest_files=table.rewrite_manifest_files,
                            expire_snapshots=table.expire_snapshots,
                            remove_orphan_files=table.remove_orphan_files,
                            rewrite_position_delete_files=table.rewrite_position_delete_files,
                            run_interval_days=(
                                table.run_interval_days
                                if table.run_interval_days
                                else 30
                            ),
                        )
                        internal_representation.configs.append(individual_config)
                    elif table.table_list:
                        for table_name in table.table_list:
                            individual_config = IndividualTableConfig(
                                base_config_name=config.base_config_name,
                                catalog=config.catalog,
                                schema_name=schema.schema_name,
                                table_name=table_name,
                                auto_optimize=table.auto_optimize,
                                rewrite_data_files=table.rewrite_data_files,
                                rewrite_manifest_files=table.rewrite_manifest_files,
                                expire_snapshots=table.expire_snapshots,
                                remove_orphan_files=table.remove_orphan_files,
                                rewrite_position_delete_files=table.rewrite_position_delete_files,
                                run_interval_days=(
                                    table.run_interval_days
                                    if table.run_interval_days
                                    else 30
                                ),
                            )
                            internal_representation.configs.append(individual_config)
                    else:
                        raise ValueError(
                            f"Table configuration in config name iceoptimizer_{config.base_config_name}.json is missing table_name or table_list."
                        )

        internal_representation.metadata_location = config.metadata_location

        print("internal representation", internal_representation)
        return internal_representation
