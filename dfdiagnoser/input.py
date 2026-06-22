import dataclasses as dc


@dc.dataclass
class FileInput:
    """Offline diagnosis from DFAnalyzer's output=file bundle.

    ``path`` is the bundle directory (mirror of the analyzer's ``output.output_dir``);
    the diagnoser reads ``<path>/facts.jsonl`` from it.
    """
    path: str


@dc.dataclass
class FactsInput:
    """Offline replay of saved analyzer fact envelopes.

    ``file_path`` is a ``.jsonl`` file (one analyzer.fact-envelope.v1 object per
    line) written by DFAnalyzer's ``output=file``. It may also be a directory of
    per-window envelope ``.json`` files.
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
