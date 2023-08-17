# Applications

This directory hosts the application layer in the standard stack [citation needed]:

1) User interface
2) Applications
3) Domain
4) Integration

In this context, stuff that goes here are user facing, actionable things that are not Neo4j specific. That's the very short description.

A longer one would be: look at the graph drop vertical mapped to layers:

1) A Neo4j procedure and a hook in the [Procedure Facade](../procedures/README.md)
2) Catalog Business Facade delegating to the Native Graph Drop application
3) Graph Store Catalog Service and little domain bits like User and GraphName
4) We haven't got an example of that yet.

Remember that code in this layer is currently used from the Neo4j integration endpoint, but that it will also be used from the eventual Arrow endpoint. Hence, it is important to ensure the code is endpoint agnostic. And that is not one-to-one synonymous with being Neo4j agnostic, but close enough.
