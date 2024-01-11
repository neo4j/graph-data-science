# Path Finding Applications

We are in the application layer, so we offer all the verticals for path finding, such as Dijkstra oin various forms, A*, Yens, you name it.

And we package them in a neat facade so that we are easily used from e.g. the Neo4j integration layer.

## Facade design

We offer facades segmented by mode, e.g. writes or estimates. 
Because these facades are the top level thing in the path finding segment of this layer, 
we should make it very useful and usable. That is their purpose. 
A single dependency that grants access.

We should capture reuse here that different UIs need. Neo4j Procedures coming in, maybe Arrow, let's make life easy for them.

Concretely, we have UI layers inject result rendering as that is bespoke. We delegate downwards for the actual computations.

Importantly, each segment-facade decides which, if any, mutate or write hooks need to be injected. We pick those from a catalog since they are all the same - relationship-centric for path finding.
