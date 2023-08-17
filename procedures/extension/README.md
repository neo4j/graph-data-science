# The GDS extension to Neo4j

Here lives the entrypoint for GDS into Neo4j. We use the extension mechanism to register a [single injectable component](../facade/README.md) with Neo4j.

This module initialises plugin state and constructs the facade. 

This facade is an umbrella over sub interfaces like the graph store catalog, community algorithms, and so forth. We do this breakdown for manageability reasons.

The GDS procedures all inject this one facade, then drill down to functional area and dispatch. We keep them dumb, simple and thin, with a hope that one day we could just generate them. We see them as markup on top of the actual procedure business methods.
