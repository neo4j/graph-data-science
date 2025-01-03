# The OpenGDS extension to Neo4j

Here live the entrypoints for OpenGDS into Neo4j. We use the extension mechanism to register an [injectable facade component](../facade/README.md) with Neo4j.
We also register per-database components.

The paradigm is, we have the facade holding functionality, accessed via a single component, registered via a global extension.
And we need per-database components as well, for example for exporter builders. That goes via a database scoped extension.

But, importantly, we only _need_ two extensions, one global, one database scoped. Important because it makes code grokable,
you have one place to go read some hopefully declarative-ish code, so that you can grok what an edition comprises.

This module initialises plugin state and constructs the facade. It is here we make edition specific choices.

NB: This module should _only_ be depended upon by the OpenGDS packaging module.
