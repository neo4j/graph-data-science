// tag::create-sample-graph[]

MERGE (a:Node{id:"A"})
MERGE (b:Node{id:"B"})
MERGE (c:Node{id:"C"})
MERGE (d:Node{id:"D"})
MERGE (e:Node{id:"E"})

MERGE (a)-[:LINK]->(b)
MERGE (b)-[:LINK]->(c)
MERGE (d)-[:LINK]->(e);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.closeness.harmonic.stream('Node', 'LINK') YIELD nodeId, centrality
RETURN nodeId,centrality
ORDER BY centrality DESC
LIMIT 20;

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.closeness.harmonic('Node', 'LINK', {writeProperty:'centrality'})
YIELD nodes,loadMillis, computeMillis, writeMillis;

// end::write-sample-graph[]

// tag::cypher-loading[]

CALL algo.closeness.harmonic(
  'MATCH (p:Node) RETURN id(p) as id',
  'MATCH (p1:Node)-[:LINK]-(p2:Node) RETURN id(p1) as source, id(p2) as target',
  {graph:'cypher', writeProperty: 'centrality'}
);

// end::cypher-loading[]

// tag::huge-projection[]

CALL algo.closeness.harmonic('Node', 'LINK', {graph:'huge'})
YIELD nodes,loadMillis, computeMillis, writeMillis;

// end::huge-projection[]
