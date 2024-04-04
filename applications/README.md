# Applications

This directory hosts the application layer in the standard stack [citation needed]:

1) User interface
2) Applications
3) Domain
4) Integration

In this context, stuff that goes here are user facing, actionable things that are not Neo4j specific. That's the very short description.

Remember why we use facades: [a facade is an object that serves as a front-facing interface masking more complex underlying or structural code](https://en.wikipedia.org/wiki/Facade_pattern).
We want to hide boring details, and instead offer easy to use, grokable stuff. As the user, you get one facade to new up and operate on.

Code in this layer is currently used from the Neo4j integration endpoint, but it will eventually also be used in other integrations (in other words, with other UIs on top).
Hence, it is important to ensure the code is endpoint agnostic. And that is not one-to-one synonymous with being Neo4j agnostic, but close enough. This quest for agnosticism is, at the time of writing, a work in progress.
