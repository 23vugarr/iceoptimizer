# responsible for generating sql for table optimization
from ice_optimizer.parser_schema import IceoptimizerConfigSchema
from typing import Dict, Set


class SqlGenerator:
    def __init__(self, config: IceoptimizerConfigSchema, table_inspector=None):
        self.config: IceoptimizerConfigSchema = config
        self.table_inspector = table_inspector

    def generate_sql(self) -> Dict[str, Set[str]]:
        """
        Generates SQL statements for table optimization based on the provided configuration.
        This method should be implemented to return the actual SQL statements.
        """
        sql_statements: Dict[str, Set[str]] = {}
        for table in self.config.configs:
            for key in table.model_dump().keys():
                if key:
                    if key in (
                        "rewrite_data_files",
                        "rewrite_manifest_files",
                        "expire_snapshots",
                        "remove_orphan_files",
                        "rewrite_position_delete_files",
                    ):
                        main_sql = f"CALL {table.catalog}.system.{key}("
                        if table.model_dump()[key]:
                            parameter_list = table.model_dump()[key]
                            options_str = "options => map("
                            params_str = (
                                f"table => '{table.schema_name}.{table.table_name}', "
                            )

                            for param, value in parameter_list.items():
                                if param.startswith("options."):
                                    options_str += f"'{param[param.index('options.') + len('options.'):]}', '{value}', "
                                else:
                                    params_str += f"'{param}' => '{value}', "

                            if len(options_str) < 20:
                                options_str = ""
                                main_sql += params_str[:-2] + ")"
                            else:
                                options_str = options_str[:-2] + ")"
                                main_sql += params_str + options_str + ")"

                            hash_of_config = hash(
                                f"{table.catalog}.{table.schema_name}.{table.table_name}.{table.run_interval_days}"
                            )

                            if hash_of_config not in sql_statements:
                                sql_statements[hash_of_config] = set()

                            sql_statements[hash_of_config].add(main_sql)

                    if key == "auto_optimize":
                        # should be implemented
                        main_sql = f"CALL {table.catalog}.system.auto_optimize("
                        main_sql += (
                            f"table => '{table.schema_name}.{table.table_name}')"
                        )

        return sql_statements
