// tag::create-sample-graph[]

MERGE (home:Page {name:'Home'})
MERGE (about:Page {name:'About'})
MERGE (product:Page {name:'Product'})
MERGE (links:Page {name:'Links'})
MERGE (a:Page {name:'Site A'})
MERGE (b:Page {name:'Site B'})
MERGE (c:Page {name:'Site C'})
MERGE (d:Page {name:'Site D'})

MERGE (home)-[:LINKS]->(about)
MERGE (about)-[:LINKS]->(home)
MERGE (product)-[:LINKS]->(home)
MERGE (home)-[:LINKS]->(product)
MERGE (links)-[:LINKS]->(home)
MERGE (home)-[:LINKS]->(links)
MERGE (links)-[:LINKS]->(a)
MERGE (a)-[:LINKS]->(home)
MERGE (links)-[:LINKS]->(b)
MERGE (b)-[:LINKS]->(home)
MERGE (links)-[:LINKS]->(c)
MERGE (c)-[:LINKS]->(home)
MERGE (links)-[:LINKS]->(d)
MERGE (d)-[:LINKS]->(home)

// end::create-sample-graph[]

// tag::stream-sample-graph[]

MATCH (home:Page {name: "Home"})
CALL algo.randomWalk.stream(id(home), 3, 1)
YIELD nodeIds

UNWIND nodeIds AS nodeId

RETURN algo.asNode(nodeId).name AS page

// end::stream-sample-graph[]

// tag::cypher-loading[]

MATCH (home:Page {name: "Home"})
CALL algo.randomWalk.stream(id(home), 5, 1, {
  nodeQuery: "MATCH (p:Page) RETURN id(p) as id",
  relationshipQuery: "MATCH (p1:Page)-[:LINKS]->(p2:Page) RETURN id(p1) as source, id(p2) as target",
  graph: "cypher"
})
YIELD nodeIds

UNWIND nodeIds AS nodeId

RETURN algo.asNode(nodeId).name AS page

// end::cypher-loading[]

// tag::random-walk-stream-yelp-social[]

CALL algo.randomWalk.stream(
  null, 10, 100,
  {graph:'cypher',
   nodeQuery:'MATCH (u:User) WHERE exists( (u)-[:FRIENDS]-() ) RETURN id(u) as id',
   relationshipQuery:'MATCH (u1:User)-[:FRIENDS]-(u2:User) RETURN id(u1) as source, id(u2) as target'}
) YIELD nodeIds
MATCH (node) where id(node) IN nodeIds
WITH nodes, collect(node {.name, .review_count, .average_stars,.useful,.yelping_since,.funny}) as info
RETURN info

// end::pagerank-stream-yelp-social[]
