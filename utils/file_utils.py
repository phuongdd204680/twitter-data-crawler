import contextlib
import os
import pathlib
import sys
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile

from constants.config import MonitoringConfig


@contextlib.contextmanager
def smart_open(filename=None, mode='w', binary=False, create_parent_dirs=True):
    fh = get_file_handle(filename, mode, binary, create_parent_dirs)

    try:
        yield fh
    finally:
        fh.close()


def get_file_handle(filename, mode='w', binary=False, create_parent_dirs=True):
    if create_parent_dirs and filename is not None:
        dirname = os.path.dirname(filename)
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
    full_mode = mode + ('b' if binary else '')
    is_file = filename and filename != '-'
    if is_file:
        fh = open(filename, full_mode)
    elif filename == '-':
        fd = sys.stdout.fileno() if mode == 'w' else sys.stdin.fileno()
        fh = os.fdopen(fd, full_mode)
    else:
        raise FileNotFoundError(filename)
    return fh


def init_last_synced_file(start_block, last_synced_block_file):
    if os.path.isfile(last_synced_block_file):
        raise ValueError(
            '{} should not exist if --start-block option is specified. '
            'Either remove the {} file or the --start-block option.'.format(last_synced_block_file,
                                                                            last_synced_block_file))
    write_last_synced_file(last_synced_block_file, start_block)


def write_last_synced_file(file, last_synced_block):
    write_to_file(file, str(last_synced_block) + '\n')


def read_last_synced_file(file):
    with smart_open(file, 'r') as file_handle:
        return int(file_handle.read())


def write_to_file(file, content):
    with smart_open(file, 'w') as file_handle:
        file_handle.write(content)


def write_monitor_logs(stream_name, synced_block, chain_id):
    """
    The write_monitor_logs function is used to write the last synced block number of a running process to a file.
    Args:
        stream_name: Identify the process
        synced_block: Update the last synced block in prometheus
        chain_id: Identify the chain

    Returns:
        Write Prometheus File Log
    
    """
    monitor_path = MonitoringConfig.MONITOR_ROOT_PATH
    registry = CollectorRegistry()
    g = Gauge('last_block_synced', 'Block synced', ['process', 'chain_id'], registry=registry)
    g.labels(stream_name, chain_id).inc(synced_block)
    _file = monitor_path + stream_name + '.prom'
    write_to_textfile(_file, registry)


def write_last_time_running_logs(stream_name, timestamp, threshold=3600):
    """
    The write_last_time_running_logs function is used to write the last time a stream was run.
    Args:
        stream_name: Identify the stream
        timestamp: Store the last time the process was run
        threshold:
    
    Returns:
        Write Prometheus File Log
    """
    monitor_path = MonitoringConfig.MONITOR_ROOT_PATH
    registry = CollectorRegistry()
    g = Gauge('last_time_run', 'Last time run', ['process', 'threshold'], registry=registry)
    g.labels(stream_name, threshold).inc(timestamp)
    _file = monitor_path + 'last_' + stream_name + '.prom'
    write_to_textfile(_file, registry)
