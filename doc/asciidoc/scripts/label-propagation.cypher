// tag::create-sample-graph[]

MERGE (nAlice:User {id:'Alice'}) SET nAlice.seed_label=52
MERGE (nBridget:User {id:'Bridget'}) SET nBridget.seed_label=21
MERGE (nCharles:User {id:'Charles'}) SET nCharles.seed_label=43
MERGE (nDoug:User {id:'Doug'}) SET nDoug.seed_label=21
MERGE (nMark:User {id:'Mark'}) SET nMark.seed_label=19
MERGE (nMichael:User {id:'Michael'}) SET nMichael.seed_label=52

MERGE (nAlice)-[:FOLLOW]->(nBridget)
MERGE (nAlice)-[:FOLLOW]->(nCharles)
MERGE (nMark)-[:FOLLOW]->(nDoug)
MERGE (nBridget)-[:FOLLOW]->(nMichael)
MERGE (nDoug)-[:FOLLOW]->(nMark)
MERGE (nMichael)-[:FOLLOW]->(nAlice)
MERGE (nAlice)-[:FOLLOW]->(nMichael)
MERGE (nBridget)-[:FOLLOW]->(nAlice)
MERGE (nMichael)-[:FOLLOW]->(nBridget)
MERGE (nCharles)-[:FOLLOW]->(nDoug);

// end::create-sample-graph[]

// tag::create-no-seed-sample-graph[]

MERGE (nAlice:User {id:'Alice'})
MERGE (nBridget:User {id:'Bridget'})
MERGE (nCharles:User {id:'Charles'})
MERGE (nDoug:User {id:'Doug'})
MERGE (nMark:User {id:'Mark'})
MERGE (nMichael:User {id:'Michael'})

MERGE (nAlice)-[:FOLLOW]->(nBridget)
MERGE (nAlice)-[:FOLLOW]->(nCharles)
MERGE (nMark)-[:FOLLOW]->(nDoug)
MERGE (nBridget)-[:FOLLOW]->(nMichael)
MERGE (nDoug)-[:FOLLOW]->(nMark)
MERGE (nMichael)-[:FOLLOW]->(nAlice)
MERGE (nAlice)-[:FOLLOW]->(nMichael)
MERGE (nBridget)-[:FOLLOW]->(nAlice)
MERGE (nMichael)-[:FOLLOW]->(nBridget)
MERGE (nCharles)-[:FOLLOW]->(nDoug);

// end::create-no-seed-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.labelPropagation.stream("User", "FOLLOW",
  {direction: "OUTGOING", iterations: 10})

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.labelPropagation('User', 'FOLLOW',
  {iterations:10, writeProperty:'partition', write:true, direction: 'OUTGOING'})
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, write, writeProperty;

// end::write-sample-graph[]


// tag::write-existing-label-sample-graph[]

CALL algo.labelPropagation('User', 'FOLLOW',
  {iterations:10,labelProperty:'seed_label', writeProperty: "partition", write:true, direction: 'OUTGOING'})
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, write, writeProperty;

// end::write-existing-label-sample-graph[]

// tag::cypher-loading[]

CALL algo.labelPropagation(
  'MATCH (p:User) RETURN id(p) as id, p.weight as weight, id(p) as value',
  'MATCH (p1:User)-[f:FRIEND]->(p2:User)
   RETURN id(p1) as source, id(p2) as target, f.weight as weight',
  {graph:'cypher',write:true});

// end::cypher-loading[]



