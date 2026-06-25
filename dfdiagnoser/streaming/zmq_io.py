"""ZMQ transport for the diagnoser (PULL consumer / PUSH producer), mirroring the
analyzer's streaming.zmq_io so analyze_zmq output=zmq feeds diagnose_zmq."""
import structlog


logger = structlog.get_logger()


def _load_zmq():
    try:
        import zmq
    except ModuleNotFoundError as exc:
        raise RuntimeError("pyzmq is not available. Install pyzmq to use ZMQ streaming.") from exc
    return zmq


def open_consumer(address: str, bind: bool = True):
    zmq = _load_zmq()
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    if bind:
        socket.bind(address)
    else:
        socket.connect(address)
    logger.info("zmq.consumer.open", address=address, bind=bind)
    return context, socket


def open_producer(address: str, bind: bool = False):
    zmq = _load_zmq()
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    if bind:
        socket.bind(address)
    else:
        socket.connect(address)
    logger.info("zmq.producer.open", address=address, bind=bind)
    return context, socket
