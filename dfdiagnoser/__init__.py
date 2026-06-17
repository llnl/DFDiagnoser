import structlog
from dataclasses import dataclass
from hydra import compose, initialize
from hydra.core.hydra_config import DictConfig, HydraConfig
from hydra.utils import instantiate
from omegaconf import OmegaConf
from typing import List, Union

from .config import init_hydra_config_store
from .diagnoser import Diagnoser
from .input import CheckpointInput, MofkaInput
from .output import ConsoleOutput, FileOutput
from .utils.log_utils import configure_logging, log_block

InputType = Union[CheckpointInput, MofkaInput]
OutputType = Union[ConsoleOutput, FileOutput]


@dataclass
class DFDiagnoserInstance:
    diagnoser: Diagnoser
    hydra_config: DictConfig
    input: InputType
    output: OutputType

    def diagnose_checkpoint(self, checkpoint_dir: str = None):
        """Diagnose the checkpoint using the configured diagnoser."""
        if checkpoint_dir is None:
            checkpoint_dir = self.input.checkpoint_dir
        # Use OmegaConf.to_object if metric_boundaries exists, otherwise use empty dict
        if 'metric_boundaries' in self.hydra_config:
            metric_boundaries = OmegaConf.to_object(self.hydra_config.metric_boundaries)
        else:
            metric_boundaries = {}
        return self.diagnoser.diagnose_checkpoint(
            checkpoint_dir=checkpoint_dir,
            metric_boundaries=metric_boundaries
        )

    def diagnose_mofka(
        self,
        group_file: str = None,
        topic_name: str = None,
        consumer_name: str = None,
        idle_timeout_sec: int = None,
        pull_timeout_ms: int = None,
        output_topic: str = None,
    ):
        """Diagnose streamed Mofka output using the configured diagnoser."""
        if not isinstance(self.input, MofkaInput):
            raise ValueError("Input is not MofkaInput")
        if group_file is None:
            group_file = self.input.group_file
        if topic_name is None:
            topic_name = self.input.topic_name
        if consumer_name is None:
            consumer_name = self.input.consumer_name
        if idle_timeout_sec is None:
            idle_timeout_sec = self.input.idle_timeout_sec
        if pull_timeout_ms is None:
            pull_timeout_ms = self.input.pull_timeout_ms
        if output_topic is None:
            output_topic = getattr(self.input, "output_topic", "")
        if "metric_boundaries" in self.hydra_config:
            metric_boundaries = OmegaConf.to_object(self.hydra_config.metric_boundaries)
        else:
            metric_boundaries = {}
        return self.diagnoser.diagnose_mofka(
            group_file=group_file,
            topic_name=topic_name,
            metric_boundaries=metric_boundaries,
            output_handler=self.output.handle_result,
            consumer_name=consumer_name,
            idle_timeout_sec=idle_timeout_sec,
            pull_timeout_ms=pull_timeout_ms,
            output_topic=output_topic,
        )

    def handle_result(self, result):
        """Handle the diagnosis result using the configured output."""
        self.output.handle_result(result)


def init_with_hydra(hydra_overrides: List[str]):
    """Initialize dfdiagnoser with Hydra configuration."""
    # Init Hydra config
    with initialize(version_base=None, config_path="configs"):
        init_hydra_config_store()
        hydra_config = compose(
            config_name="config",
            overrides=hydra_overrides,
            return_hydra_config=True,
        )
    HydraConfig.instance().set_config(hydra_config)

    # Configure structlog + stdlib logging
    log_file = f"{hydra_config.hydra.run.dir}/{hydra_config.hydra.job.name}.log"
    log_level = "debug" if hydra_config.debug else "info"
    configure_logging(log_file=log_file, level=log_level)
    log = structlog.get_logger()
    log.info("Starting dfdiagnoser")

    # Setup diagnoser
    with log_block("Diagnoser setup"):
        diagnoser = instantiate(hydra_config.diagnoser)

    # Setup input and output
    with log_block("Input and output setup"):
        input_instance = instantiate(hydra_config.input)
        output_instance = instantiate(hydra_config.output)

    return DFDiagnoserInstance(
        diagnoser=diagnoser,
        hydra_config=hydra_config,
        input=input_instance,
        output=output_instance,
    )


__all__ = [
    "InputType",
    "OutputType",
    "CheckpointInput",
    "MofkaInput",
    "ConsoleOutput",
    "FileOutput",
    "DFDiagnoserInstance",
    "init_with_hydra",
]
