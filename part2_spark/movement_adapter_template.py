"""Proposed benchmark adapter contract, NOT Joshua's movement implementation.

Copy this to a real adapter only after inspecting Joshua's delivered code.
Imports must be safe: no Spark startup, computation, or database/file writes.
"""

BENCHMARK_ADAPTER_VERSION = 1
BENCHMARK_ADAPTER_READY = False


def build_features(*, spark, tracking, plays, injuries):
    """Return {'play_features': DataFrame, 'movement_summary': DataFrame}.

    Use the supplied tracking subset for Joshua's unchanged calculations.
    Do not reread full tracking data, replace the session/master, run actions,
    or write outputs. The benchmark driver owns materialization and timing.
    Required output columns are documented in the shared schema.sql.
    """
    raise NotImplementedError(
        "Joshua's movement job is not available in the inspected branches. "
        "This template intentionally contains no movement calculations."
    )
