from pydantic import BaseModel
from typing import Optional, Dict, List


# start of config json schema definitions
class TableConfig(BaseModel):
    table_name: Optional[str] = None
    table_list: Optional[List[str]] = None
    run_interval_days: Optional[int] = 30

    auto_optimize: Optional[bool] = False
    rewrite_data_files: Optional[Dict] = None
    rewrite_manifest_files: Optional[Dict] = None
    expire_snapshots: Optional[Dict] = None
    remove_orphan_files: Optional[Dict] = None
    rewrite_position_delete_files: Optional[Dict] = None


class SchemaConfig(BaseModel):
    schema_name: str
    tables: List[TableConfig]


class BaseConfig(BaseModel):
    base_config_name: Optional[str] = None
    catalog: str
    metadata_location: str
    schemas: List[SchemaConfig]


# start of logical design of individual config in iceoptimizer
class IndividualTableConfig(BaseModel):
    base_config_name: str
    catalog: str
    schema_name: str
    table_name: str
    run_interval_days: int

    auto_optimize: Optional[bool] = False
    rewrite_data_files: Optional[Dict] = None
    rewrite_manifest_files: Optional[Dict] = None
    expire_snapshots: Optional[Dict] = None
    remove_orphan_files: Optional[Dict] = None
    rewrite_position_delete_files: Optional[Dict] = None


class IceoptimizerConfigSchema(BaseModel):
    metadata_location: str
    configs: List[IndividualTableConfig]
