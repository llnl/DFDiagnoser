import dataclasses as dc


@dc.dataclass
class CheckpointInput:
    checkpoint_dir: str


@dc.dataclass
class FactsInput:
    """Offline replay of saved analyzer fact envelopes.

    ``file_path`` mirrors DFAnalyzer's ``output=facts`` ``file_path``: a ``.jsonl``
    file (one analyzer.fact-envelope.v1 object per line). It may also be a
    directory of per-window envelope ``.json`` files.
    """
    file_path: str


@dc.dataclass
class MofkaInput:
    group_file: str
    topic_name: str
    consumer_name: str = ""
    idle_timeout_sec: int = 0
    pull_timeout_ms: int = 1000
    output_topic: str = ""
