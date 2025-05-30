from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import yaml
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class SparkConfig:
    app_name: str = "nitrogen"
    master: str = "local[*]"
    configs: Dict[str, Any] = field(default_factory=dict)

@dataclass_json
@dataclass
class ComponentConfig:
    type: str
    options: Dict[str, Any]

@dataclass_json
@dataclass
class JobConfig:
    name: str
    description: str
    extract: ComponentConfig
    transform: List[ComponentConfig]
    load: ComponentConfig
    dependencies: Dict[str, ComponentConfig] = field(default_factory=dict)
    spark_config: Optional[SparkConfig] = None

    def __post_init__(self):
        if self.spark_config is None:
            self.spark_config = SparkConfig()

def load_job_config(config_path: str) -> JobConfig:
    """Load and parse job configuration from YAML file"""
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    return JobConfig.from_dict(config_dict) 