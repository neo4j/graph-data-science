// tag::create-sample-graph[]

MERGE (nAlice:User {id:'Alice'})
MERGE (nBridget:User {id:'Bridget'})
MERGE (nCharles:User {id:'Charles'})
MERGE (nDoug:User {id:'Doug'})
MERGE (nMark:User {id:'Mark'})
MERGE (nMichael:User {id:'Michael'})

MERGE (nAlice)-[:FOLLOW]->(nBridget)
MERGE (nAlice)-[:FOLLOW]->(nCharles)
MERGE (nMark)-[:FOLLOW]->(nDoug)
MERGE (nMark)-[:FOLLOW]->(nMichael)
MERGE (nBridget)-[:FOLLOW]->(nMichael)
MERGE (nDoug)-[:FOLLOW]->(nMark)
MERGE (nMichael)-[:FOLLOW]->(nAlice)
MERGE (nAlice)-[:FOLLOW]->(nMichael)
MERGE (nBridget)-[:FOLLOW]->(nAlice)
MERGE (nMichael)-[:FOLLOW]->(nBridget);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.scc.stream('User','FOLLOW')
YIELD nodeId, partition

RETURN algo.asNode(nodeId).id AS name, partition

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.scc('User','FOLLOW', {write:true,partitionProperty:'partition'})
YIELD loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize;

// end::write-sample-graph[]

// tag::get-largest-component[]

MATCH (u:User)
RETURN u.partition as partition,count(*) as size_of_partition
ORDER by size_of_partition DESC
LIMIT 1

// end::get-largest-component[]

// tag::cypher-loading[]

CALL algo.scc(
  'MATCH (u:User) RETURN id(u) as id',
  'MATCH (u1:User)-[:FOLLOW]->(u2:User) RETURN id(u1) as source,id(u2) as target',
  {write:true,graph:'cypher'})
YIELD loadMillis, computeMillis, writeMillis;

// end::cypher-loading[]

// tag::huge-projection[]

CALL algo.scc('User','FOLLOW', {graph:'huge'})
YIELD loadMillis, computeMillis, writeMillis, setCount;

// end::huge-projection[]
