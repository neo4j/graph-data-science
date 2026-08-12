# Algorithms machinery

This is the machinery that underpins algorithm execution.
The machinery is shared, because all algorithms follow a common flow of validation, loading, execution, result application and result rendering.
And they are all instrumented with timings, logging, checks, ...

We have exactly one machine for executing algorithms - no duplication. It is a generic engine that takes many parameters - the superset of all parameters for all steps, and performs the series of steps in order:

1) At the very bottom, an algorithms is executed.
1) We take timestamps before and after algorithm execution, and record computation time.
1) We also instrument the algorithm with telemetry logging.
1) Ditto algorithm metrics.
1) There is a memory guard in front
1) We have to actually construct the algorithm before we can run it
1) We apply a side effect once we have the result. Side effects include a no-op for when we stream; or writing the result in other modes
1) We take the result, and metadata from apply the side effect, and render a result or summary to the caller
1) All this forms one synchronous job, we call that the _computation_. We launch the computation asynchronously
1) We obviously had to fetch the graph beforehand, a process that might involve algorithm-specific validation
1) We instrument these steps with timing recodings and logging

Now, callers _can choose_ to block and wait for this job at this point. Cypher-via-plugin is an example of that.
But other calling modes can take advantage of the asynchronous nature of this machine, and do other work while periodically checking for completion.
