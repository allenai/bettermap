#!/usr/bin/python3
import io
import sys
from concurrent.futures import ThreadPoolExecutor

import itertools
import multiprocessing as mp
from multiprocessing.connection import Connection
from multiprocessing.context import ForkProcess
from typing import Iterable, List, Optional, Any, Dict, Tuple

import dill

from queue import Queue
from threading import Thread


mpctx = mp.get_context("fork")


def threaded_generator(g, maxsize: int = 16):
    q: Queue = Queue(maxsize=maxsize)

    sentinel = object()

    def fill_queue():
        try:
            for value in g:
                q.put(value)
        finally:
            q.put(sentinel)

    thread = Thread(name=repr(g), target=fill_queue, daemon=True)
    thread.start()

    yield from iter(q.get, sentinel)


def slices(n: int, i: Iterable) -> Iterable[List]:
    i = iter(i)
    while True:
        s = list(itertools.islice(i, n))
        if len(s) > 0:
            yield s
        else:
            break


def map_per_process(
    fn,
    input_sequence: Iterable,
    *,
    serialization_items: Optional[List[Any]] = None,
    parallelism: int = mpctx.cpu_count()
) -> Iterable:
    if serialization_items is not None and len(serialization_items) > 0:
        serialization_ids = [id(o) for o in serialization_items]
        class MapPickler(dill.Pickler):
            def persistent_id(self, obj):
                try:
                    return serialization_ids.index(id(obj))
                except ValueError:
                    return None
        class MapUnpickler(dill.Unpickler):
            def persistent_load(self, pid):
                return serialization_items[pid]
    else:
        MapPickler = dill.Pickler  # type: ignore
        MapUnpickler = dill.Unpickler  # type: ignore
    def pickle(o: Any) -> bytes:
        with io.BytesIO() as buffer:
            pickler = MapPickler(buffer)
            pickler.dump(o)
            return buffer.getvalue()
    def unpickle(b: bytes) -> Any:
        with io.BytesIO(b) as buffer:
            unpickler = MapUnpickler(buffer)
            return unpickler.load()

    pipeno_to_pipe: Dict[int, Connection] = {}
    pipeno_to_process: Dict[int, ForkProcess] = {}

    def process_one_item(send_pipe: Connection, item):
        try:
            processed_item = fn(item)
        except Exception as e:
            import traceback
            send_pipe.send((None, (e, traceback.format_exc())))
        else:
            send_pipe.send((pickle(processed_item), None))
        send_pipe.close()

    def yield_from_pipes(pipes: List[Connection]):
        for pipe in pipes:
            result, error = pipe.recv()
            pipeno = pipe.fileno()
            del pipeno_to_pipe[pipeno]
            pipe.close()

            process = pipeno_to_process[pipeno]
            process.join()
            del pipeno_to_process[pipeno]

            if error is None:
                yield unpickle(result)
            else:
                e, tb = error
                sys.stderr.write("".join(tb))
                raise e

    try:
        for item in input_sequence:
            receive_pipe, send_pipe = mpctx.Pipe(duplex=False)
            process = mpctx.Process(target=process_one_item, args=(send_pipe, item))
            pipeno_to_pipe[receive_pipe.fileno()] = receive_pipe
            pipeno_to_process[receive_pipe.fileno()] = process
            process.start()

            # read out the values
            timeout = 0 if len(pipeno_to_process) < parallelism else None
            # If we have fewer processes going than we have CPUs, we just pick up the values
            # that are done. If we are at the process limit, we wait until one of them is done.
            ready_pipes = mp.connection.wait(pipeno_to_pipe.values(), timeout=timeout)
            yield from yield_from_pipes(ready_pipes)  # type: ignore

        # yield the rest of the items
        while len(pipeno_to_process) > 0:
            ready_pipes = mp.connection.wait(pipeno_to_pipe.values(), timeout=None)
            yield from yield_from_pipes(ready_pipes)  # type: ignore

    finally:
        for process in pipeno_to_process.values():
            if process.is_alive():
                process.terminate()


def ordered_map_per_process(
    fn,
    input_sequence: Iterable,
    *,
    serialization_items: Optional[List[Any]] = None
) -> Iterable:
    def process_item(item):
        index, item = item
        return index, fn(item)
    results_with_index = map_per_process(
        process_item,
        enumerate(input_sequence),
        serialization_items=serialization_items)

    expected_index = 0
    items_in_wait: List[Tuple[int, Any]] = []
    for item in results_with_index:
        index, result = item
        if index == expected_index:
            yield result
            expected_index = index + 1

            items_in_wait.sort(reverse=True)
            while len(items_in_wait) > 0 and items_in_wait[-1][0] == expected_index:
                index, result = items_in_wait.pop()
                yield result
                expected_index = index + 1
        else:
            items_in_wait.append(item)


def ordered_map_per_thread(
    fn,
    input_sequence: Iterable,
    *,
    parallelism: int = mpctx.cpu_count()
) -> Iterable:
    executor = ThreadPoolExecutor(max_workers=parallelism)
    input_sequence = (executor.submit(fn, item) for item in input_sequence)
    input_sequence = threaded_generator(input_sequence, maxsize=parallelism)
    for future in input_sequence:
        yield future.result()
    executor.shutdown()


def map_in_chunks(
    fn,
    input_sequence: Iterable,
    *,
    chunk_size: int = 10,
    serialization_items: Optional[List[Any]] = None
) -> Iterable:
    def process_chunk(chunk: List) -> List:
        return list(map(fn, chunk))

    processed_chunks = map_per_process(
        process_chunk,
        slices(chunk_size, input_sequence),
        serialization_items=serialization_items)
    for processed_chunk in processed_chunks:
        yield from processed_chunk


def ordered_map_in_chunks(
    fn,
    input_sequence: Iterable,
    *,
    chunk_size: int = 10,
    serialization_items: Optional[List[Any]] = None
) -> Iterable:
    def process_chunk(chunk: List) -> List:
        return list(map(fn, chunk))

    processed_chunks = ordered_map_per_process(
        process_chunk,
        slices(chunk_size, input_sequence),
        serialization_items=serialization_items)
    for processed_chunk in processed_chunks:
        yield from processed_chunk
