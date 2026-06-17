import hydra
# import signal
import os
import structlog
from hydra.core.hydra_config import HydraConfig
from hydra.utils import instantiate
from omegaconf import DictConfig
from pathlib import Path

from . import InputType, OutputType
from .config import init_hydra_config_store
from .diagnoser import Diagnoser
from .input import CheckpointInput, MofkaInput
from .utils.log_utils import configure_logging, console_block, log_block


init_hydra_config_store()


@hydra.main(config_path="configs", config_name="config", version_base="1.1")
def main(cfg: DictConfig):
    # Configure structlog + stdlib logging
    hydra_config = HydraConfig.get()
    log_file = f"{hydra_config.runtime.output_dir}/{hydra_config.job.name}.log"
    log_level = "debug" if cfg.debug else "info"
    configure_logging(log_file=log_file, level=log_level)
    log = structlog.get_logger()
    log.info("Starting DFDiagnoser")

    with console_block("Diagnoser setup"):
        diagnoser: Diagnoser = instantiate(cfg.diagnoser)
    
    with log_block("Input and output setup"):
        input: InputType = instantiate(cfg.input)
        output: OutputType = instantiate(cfg.output)

    if isinstance(input, CheckpointInput):
        # Resolve checkpoint_dir to absolute path to handle working directory changes
        checkpoint_dir = Path(input.checkpoint_dir).resolve()
        diagnosis_result = diagnoser.diagnose_checkpoint(str(checkpoint_dir))
        with console_block("Output"):
            output.handle_result(diagnosis_result)
    elif isinstance(input, MofkaInput):
        diagnoser.diagnose_mofka(
            group_file=input.group_file,
            topic_name=input.topic_name,
            output_handler=output.handle_result,
        )
    # elif isinstance(input, ZMQInput):
    #     diagnosis_stream = diagnoser.diagnose_zmq(input.address)
    #     diagnosis_stream.start()
    #     print("Streaming diagnosis started. Press Ctrl+C to exit.")
    #     try:
    #         signal.pause()
    #     except KeyboardInterrupt:
    #         print("\nShutting down streaming diagnosis...")
    #     finally:
    #         diagnosis_stream.stop()
    else:
        raise ValueError(f"Invalid input type: {type(input)}")


if __name__ == "__main__":
    main()
