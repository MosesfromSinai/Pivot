"""Benchmark Joshua's shared transformations using only the supplied inputs."""

from importlib import import_module

BENCHMARK_ADAPTER_VERSION = 1
BENCHMARK_ADAPTER_READY = True


def build_features(*, spark, tracking, plays, injuries):
    return import_module("02_movement_features").build_features(
        spark=spark, tracking=tracking, plays=plays, injuries=injuries,
    )
