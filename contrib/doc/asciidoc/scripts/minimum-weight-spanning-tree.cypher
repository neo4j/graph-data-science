// tag::create-sample-graph[]

MERGE (a:Place {id:"A"})
MERGE (b:Place {id:"B"})
MERGE (c:Place {id:"C"})
MERGE (d:Place {id:"D"})
MERGE (e:Place {id:"E"})
MERGE (f:Place {id:"F"})
MERGE (g:Place {id:"G"})

MERGE (d)-[:LINK {cost:4}]->(b)
MERGE (d)-[:LINK {cost:6}]->(e)
MERGE (b)-[:LINK {cost:1}]->(a)
MERGE (b)-[:LINK {cost:3}]->(c)
MERGE (a)-[:LINK {cost:2}]->(c)
MERGE (c)-[:LINK {cost:5}]->(e)
MERGE (f)-[:LINK {cost:1}]->(g);

// end::create-sample-graph[]

// tag::write-sample-minst-graph[]

MATCH (n:Place {id:"D"})
CALL algo.spanningTree.minimum('Place', 'LINK', 'cost', id(n),
  {write:true, writeProperty:"MINST"})
YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount
RETURN loadMillis, computeMillis, writeMillis, effectiveNodeCount;

// end::write-sample-minst-graph[]

// tag::query-sample-minst-graph[]

MATCH path = (n:Place {id:"D"})-[:MINST*]-()
WITH relationships(path) AS rels
UNWIND rels AS rel
WITH DISTINCT rel AS rel
RETURN startNode(rel).id AS source, endNode(rel).id AS destination, rel.cost AS cost

// end::query-sample-minst-graph[]


// tag::write-sample-maxst-graph[]


MATCH (n:Place{id:"D"}) 
CALL algo.spanningTree.maximum('Place', 'LINK', 'cost', id(n),
  {write:true, writeProperty:"MAXST"})
YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount
RETURN loadMillis,computeMillis, writeMillis, effectiveNodeCount;

// end::write-sample-maxst-graph[]


// tag::write-sample-kmaxst-graph[]

MATCH (n:Place{id:"D"}) 
CALL algo.spanningTree.kmax('Place', 'LINK', 'cost', id(n), 3,
  {writeProperty:"kmaxst"})
YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount
RETURN loadMillis,computeMillis,writeMillis, effectiveNodeCount;

// end::write-sample-kmaxst-graph[]

// tag::write-sample-kminst-graph[]

MATCH (n:Place{id:"D"}) 
CALL algo.spanningTree.kmin('Place', 'LINK', 'cost',id(n), 3,
  {writeProperty:"kminst"})
YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount
RETURN loadMillis,computeMillis,writeMillis, effectiveNodeCount;


// end::write-sample-kminst-graph[]
