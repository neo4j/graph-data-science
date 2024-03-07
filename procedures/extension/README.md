# The OpenGDS extension to Neo4j

Here lives the entrypoint for OpenGDS into Neo4j. We use the extension mechanism to register a [single injectable component](../facade/README.md) with Neo4j.

This module initialises plugin state and constructs the facade. It is here we make edition specific choices.

NB: This module should _only_ be depended upon by the OpenGDS packaging module.
