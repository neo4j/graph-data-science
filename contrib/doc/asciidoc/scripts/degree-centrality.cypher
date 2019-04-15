// tag::create-sample-graph[]

MERGE (nAlice:User {id:'Alice'})
MERGE (nBridget:User {id:'Bridget'})
MERGE (nCharles:User {id:'Charles'})
MERGE (nDoug:User {id:'Doug'})
MERGE (nMark:User {id:'Mark'})
MERGE (nMichael:User {id:'Michael'})

MERGE (nAlice)-[:FOLLOWS]->(nDoug)
MERGE (nAlice)-[:FOLLOWS]->(nBridget)
MERGE (nAlice)-[:FOLLOWS]->(nCharles)
MERGE (nMark)-[:FOLLOWS]->(nDoug)
MERGE (nMark)-[:FOLLOWS]->(nMichael)
MERGE (nBridget)-[:FOLLOWS]->(nDoug)
MERGE (nCharles)-[:FOLLOWS]->(nDoug)
MERGE (nMichael)-[:FOLLOWS]->(nDoug)

// end::create-sample-graph[]

// tag::create-sample-weighted-graph[]

MERGE (nAlice:User {id:'Alice'})
MERGE (nBridget:User {id:'Bridget'})
MERGE (nCharles:User {id:'Charles'})
MERGE (nDoug:User {id:'Doug'})
MERGE (nMark:User {id:'Mark'})
MERGE (nMichael:User {id:'Michael'})

MERGE (nAlice)-[:FOLLOWS {score: 1}]->(nDoug)
MERGE (nAlice)-[:FOLLOWS {score: 2}]->(nBridget)
MERGE (nAlice)-[:FOLLOWS {score: 5}]->(nCharles)
MERGE (nMark)-[:FOLLOWS {score: 1.5}]->(nDoug)
MERGE (nMark)-[:FOLLOWS {score: 4.5}]->(nMichael)
MERGE (nBridget)-[:FOLLOWS {score: 1.5}]->(nDoug)
MERGE (nCharles)-[:FOLLOWS {score: 2}]->(nDoug)
MERGE (nMichael)-[:FOLLOWS {score: 1.5}]->(nDoug)

// end::create-sample-weighted-graph[]

// tag::stream-sample-graph-followers[]
CALL algo.degree.stream("User", "FOLLOWS", {direction: "incoming"})
YIELD nodeId, score
RETURN algo.asNode(nodeId).id AS name, score AS followers
ORDER BY followers DESC
// end::stream-sample-graph-followers[]

// tag::write-sample-graph-followers[]
CALL algo.degree("User", "FOLLOWS", {direction: "incoming", writeProperty: "followers"})
// end::write-sample-graph-followers[]

// tag::stream-sample-graph-following[]
CALL algo.degree.stream("User", "FOLLOWS", {direction: "outgoing"})
YIELD nodeId, score
RETURN algo.asNode(nodeId).id AS name, score AS following
ORDER BY following DESC
// end::stream-sample-graph-following[]

// tag::write-sample-graph-following[]
CALL algo.degree("User", "FOLLOWS", {direction: "outgoing", writeProperty: "following"})
// end::write-sample-graph-following[]



// tag::stream-sample-weighted-graph-followers[]
CALL algo.degree.stream("User", "FOLLOWS", {direction: "incoming", weightProperty: "score"})
YIELD nodeId, score
RETURN algo.asNode(nodeId).id AS name, score AS weightedFollowers
ORDER BY followers DESC
// end::stream-sample-weighted-graph-followers[]

// tag::write-sample-weighted-graph-followers[]
CALL algo.degree("User", "FOLLOWS",
  {direction: "incoming", writeProperty: "weightedFollowers", weightProperty: "score"})
// end::write-sample-weighted-graph-followers[]

// tag::huge-projection[]

CALL algo.degree('User','FOLLOWS', {graph:'huge'});

// end::huge-projection[]

// tag::cypher-loading[]

CALL algo.degree(
  'MATCH (u:User) RETURN id(u) as id',
  'MATCH (u1:User)<-[:FOLLOWS]-(u2:User) RETURN id(u1) as source, id(u2) as target',
  {graph:'cypher', write: true, writeProperty: "followers"}
)

// end::cypher-loading[]

// tag::cypher-loading-following[]

CALL algo.degree(
  'MATCH (u:User) RETURN id(u) as id',
  'MATCH (u1:User)-[:FOLLOWS]->(u2:User) RETURN id(u1) as source, id(u2) as target',
  {graph:'cypher', write: true, writeProperty: "following"}
)

// end::cypher-loading-following[]