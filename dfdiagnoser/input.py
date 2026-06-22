import dataclasses as dc


@dc.dataclass
class FileInput:
    """Offline diagnosis from DFAnalyzer's output=file bundle.

    ``path`` is the bundle directory (mirror of the analyzer's ``output.output_dir``);
    the diagnoser reads ``<path>/facts.jsonl`` from it.
    """
    path: str


@dc.dataclass
class MofkaInput:
    group_file: str
    topic_name: str
    consumer_name: str = ""
    idle_timeout_sec: int = 0
    pull_timeout_ms: int = 1000
    output_topic: str = ""
