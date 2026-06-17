import os
import pytest
import shutil
import subprocess
import time
import uuid


def _mofka_available():
    try:
        import mochi.mofka.client as mofka  # noqa: F401
    except ModuleNotFoundError:
        return False
    return True


@pytest.fixture(scope="module")
def bedrock_mofka():
    if not _mofka_available():
        pytest.skip("mochi.mofka is not installed")

    bedrock = shutil.which("bedrock")
    if not bedrock:
        pytest.skip("bedrock not found in PATH")

    tests_root = os.path.abspath(os.path.dirname(__file__))
    config_path = os.path.join(tests_root, "mofka.config.json")
    group_file = os.path.join(tests_root, "mofka.group.json")
    log_path = os.path.join(tests_root, "mofka_server.log")

    if os.path.exists(group_file):
        os.remove(group_file)

    proc = subprocess.Popen(
        [bedrock, "tcp", "-c", config_path, "-v", "trace"],
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
        cwd=tests_root,
    )

    timeout = time.time() + 10
    while time.time() < timeout:
        if os.path.exists(group_file):
            break
        time.sleep(0.1)
    if not os.path.exists(group_file):
        proc.terminate()
        pytest.fail("mofka.group.json was not created")

    topic_name = f"dfdiagnoser_test_{uuid.uuid4().hex}"
    subprocess.check_call(
        [
            "python",
            "-m",
            "mochi.mofka.mofkactl",
            "topic",
            "create",
            topic_name,
            "--groupfile",
            group_file,
        ]
    )
    subprocess.check_call(
        [
            "python",
            "-m",
            "mochi.mofka.mofkactl",
            "partition",
            "add",
            topic_name,
            "--type",
            "memory",
            "--rank",
            "0",
            "--groupfile",
            group_file,
        ]
    )

    yield group_file, topic_name

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    if os.path.exists(group_file):
        os.remove(group_file)
