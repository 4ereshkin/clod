"""
Workers for point cloud processing.

This package contains entry points for Temporal workers.  A worker
connects to the Temporal server, polls a task queue for workflow and
activity tasks and executes them using the definitions provided in the
``point_cloud.temporal`` package.

Running a worker from the command line is typically as simple as:

.. code-block:: bash

    python -m point_cloud.temporal.workers.worker_orchestrator

See the individual modules in this package for more details.
"""

__all__ = ["worker_orchestrator"]