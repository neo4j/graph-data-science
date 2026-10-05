# Algorithms

This directory holds the GDS algorithm catalogue, the crown jewels.

In here you will find the catalogue of algorithms across several modes, grouped
by category. Also, we have the common machinery for executing algorithms and all
the associated instrumentation.

You will recognise a pattern in the machinery:

1. There is an algorithm at the bottom, grouped by category - `*Algorithms`, e.g. `CentralityAlgorithms`.
2. On top of that, it's associated progress tracking configuration -
   `Tracked*Algorithms`.
3. Lastly the algorithm wrapped in ancillaries like label, estimation, ETL/
   validation - `Instrumented*Algorithms`.

The latter you should see as the facade to algorithms: asynchronous endpoints
that you can build modes on top of.

The modes are things like stream or mutate, where you _immediately_ turn the
calls synchronous and inject side effects and rendering; or other mods where you
might take advantage of asynchronicity. You can read about those designs
elsewhere.
