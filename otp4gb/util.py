# -*- coding: utf-8 -*-
"""Utility functions for OTP4GB."""

import concurrent.futures
import datetime
import logging
import multiprocessing
import time
from typing import Callable, Iterator, Optional, TypeVar

LOG = logging.getLogger(__name__)
TEXT_ENCODING = "utf-8"

T = TypeVar("T")
A = TypeVar("A")


class Timer:
    def __init__(self):
        self.tic = time.perf_counter()

    def __str__(self) -> str:
        toc = time.perf_counter()
        return str(datetime.timedelta(seconds=(toc - self.tic)))


def chunker(input_list, chunk_size=100):
    return [
        input_list[i : i + chunk_size] for i in range(0, len(input_list), chunk_size)
    ]


def multiprocess_function(
    processes: int, args: list[A], func: Callable[[A], T], chunksize: int = 1
) -> Iterator[T]:
    """Generator to run a give function with multiple processes.

    Parameters
    ----------
    processes : int
        Number of processes to create, if 0 then `func` is called
        in the main process.
    args : list[A]
        List of arguments to run the function on.
    func : Callable[[A], T]
        Function to call.
    chunksize : int, default 1
        Size of chunks to split `args` into when creating processes.

    Yields
    ------
    T
        Result from each run of `func`.
    """
    if processes == 0:
        for a in args:
            yield func(a)

    else:
        with multiprocessing.Pool(processes) as p:
            results = p.imap_unordered(func, args, chunksize=chunksize)

            for r in results:
                yield r


def multithread_function(
    workers: int,
    func: Callable[..., T],
    args: list[A],
    shared_kwargs: Optional[dict] = None,
) -> Iterator[T]:
    """Generator for running a function on multiple threads.

    Parameters
    ----------
    workers : int
        Number of thread to create, if 0 then no new threads
        are created.
    func : Callable[..., T]
        Function to run.
    args : list[A]
        List of arguments for `func`.
    shared_kwargs : dict, optional
        Any other arguments which are the same for every run of `func`.

    Yields
    ------
    T
        Result from `func`.
    """
    if shared_kwargs is None:
        shared_kwargs = {}

    if workers == 0:
        for a in args:
            yield func(a, **shared_kwargs)

    else:
        with concurrent.futures.ThreadPoolExecutor(
            workers, thread_name_prefix=func.__name__
        ) as executor:
            futures = [executor.submit(func, a, **shared_kwargs) for a in args]

            for r in concurrent.futures.as_completed(futures):
                try:
                    result = r.result()
                    yield result
                except Exception:
                    LOG.error(
                        "Error in thread, shutting down all threads", exc_info=True
                    )
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise
