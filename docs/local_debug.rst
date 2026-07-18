.. SPDX-License-Identifier: Apache-2.0

Local development and debugging
===============================

Embedded local cluster
----------------------

Streaming execution calls ``ray.init()`` when no Ray runtime is active. This
starts a local cluster whose lifetime follows the Python process. You can also
start Ray first and attach to it. Pre-initialize Ray in the application when
you need custom dashboard, metrics-export, address, or resource settings;
Klein for Ray will reuse the active runtime:

.. code-block:: bash

   ray start --head --include-dashboard=true
   python examples/quick_start.py

Use ``ray stop --force`` after interrupted local integration tests if Ray
processes remain.

In-process debug mode
---------------------

Set ``RAY_KLEIN_DEBUG=1`` to replace Ray actors with local Python objects and
dedicated event-loop threads. This is useful for IDE breakpoints in streaming
operators, but it does not validate serialization, placement, resource
scheduling, process isolation, or Ray failure recovery.

.. code-block:: bash

   RAY_KLEIN_DEBUG=1 python examples/quick_start.py

Run the integration suite without debug mode before relying on a pipeline in
production.

Logging
-------

Libraries should not configure application logging on import. Applications can
call ``ray.klein.configure_logging()`` explicitly. Set ``RAY_KLEIN_LOGGING_CONFIG``
to load a custom YAML configuration.
