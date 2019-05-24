// tag::create-sample-graph[]

MERGE (a:Node{id:"A"})
MERGE (b:Node{id:"B"})
MERGE (c:Node{id:"C"})
MERGE (d:Node{id:"D"})
MERGE (e:Node{id:"E"})

MERGE (a)-[:LINK]->(b)
MERGE (b)-[:LINK]->(a)
MERGE (b)-[:LINK]->(c)
MERGE (c)-[:LINK]->(b)
MERGE (c)-[:LINK]->(d)
MERGE (d)-[:LINK]->(c)
MERGE (d)-[:LINK]->(e)
MERGE (e)-[:LINK]->(d);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.closeness.stream('Node', 'LINK')
YIELD nodeId, centrality

RETURN algo.asNode(nodeId).id AS node, centrality
ORDER BY centrality DESC
LIMIT 20;

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.closeness('Node', 'LINK', {write:true, writeProperty:'centrality'})
YIELD nodes,loadMillis, computeMillis, writeMillis;

// end::write-sample-graph[]

// tag::cypher-loading[]

CALL algo.closeness(
  'MATCH (p:Node) RETURN id(p) as id',
  'MATCH (p1:Node)-[:LINK]->(p2:Node) RETURN id(p1) as source, id(p2) as target',
  {graph:'cypher', write: true}
);

// end::cypher-loading[]


// tag::huge-projection[]

CALL algo.closeness('Node', 'LINK', {graph:'huge'})
YIELD nodes,loadMillis, computeMillis, writeMillis;

// end::huge-projection[]
