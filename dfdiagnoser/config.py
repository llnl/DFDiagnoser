import dataclasses as dc
from hydra.core.config_store import ConfigStore
from omegaconf import MISSING
from typing import List, Optional


@dc.dataclass
class InputConfig:
    pass


@dc.dataclass
class CheckpointInputConfig(InputConfig):
    _target_: str = "dfdiagnoser.input.CheckpointInput"
    checkpoint_dir: str = MISSING


@dc.dataclass
class MofkaInputConfig(InputConfig):
    _target_: str = "dfdiagnoser.input.MofkaInput"
    group_file: str = MISSING
    topic_name: str = MISSING
    consumer_name: str = ""
    idle_timeout_sec: int = 0
    pull_timeout_ms: int = 1000
    output_topic: str = ""


@dc.dataclass
class OutputConfig:
    pass


@dc.dataclass
class FileOutputConfig(OutputConfig):
    _target_: str = "dfdiagnoser.output.FileOutput"
    output_dir: Optional[str] = None
    output_format: str = "json"


@dc.dataclass
class ConsoleOutputConfig(FileOutputConfig):
    _target_: str = "dfdiagnoser.output.ConsoleOutput"
    show_debug: Optional[bool] = False
    show_header: Optional[bool] = True


@dc.dataclass
class RuleReasonDefinitionConfig:
    condition: str
    message: str


@dc.dataclass
class RuleDefinitionConfig:
    name: str
    condition: str
    reasons: Optional[List[RuleReasonDefinitionConfig]] = None


@dc.dataclass
class DiagnoserConfig:
    _target_: str = "dfdiagnoser.diagnoser.Diagnoser"


def init_hydra_config_store() -> ConfigStore:
    cs = ConfigStore.instance()
    cs.store(group="diagnoser", name="default", node=DiagnoserConfig)
    cs.store(group="input", name="checkpoint", node=CheckpointInputConfig)
    cs.store(group="input", name="mofka", node=MofkaInputConfig)
    cs.store(group="output", name="console", node=ConsoleOutputConfig)
    cs.store(group="output", name="file", node=FileOutputConfig)
    return cs
