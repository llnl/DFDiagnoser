import dataclasses as dc


@dc.dataclass
class CheckpointInput:
    checkpoint_dir: str


@dc.dataclass
class MofkaInput:
    group_file: str
    topic_name: str
    consumer_name: str = ""
    idle_timeout_sec: int = 0
    pull_timeout_ms: int = 1000
    output_topic: str = ""
