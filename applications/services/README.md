# Application Services

Here we can stick behaviour that we use between the various application modules, but which does not belong in any particular application module. Calling it "services" to avoid the tired "common" or "utilities". It is application layer reuse.

Beware dependencies: this should only be depended on by application modules; it is internal to the application layer.
