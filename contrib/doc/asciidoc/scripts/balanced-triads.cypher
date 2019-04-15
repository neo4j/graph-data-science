// tag::create-sample-graph[]

MERGE (a:Person {name:'Anna'})
MERGE (b:Person {name:'Dolores'})
MERGE (c:Person {name:'Matt'})
MERGE (d:Person {name:'Larry'})
MERGE (e:Person {name:'Stefan'})
MERGE (f:Person {name:'Sophia'})
MERGE (g:Person {name:'Robin'})
MERGE (a)-[:TYPE {weight:1.0}]->(b)
MERGE (a)-[:TYPE {weight:-1.0}]->(c)
MERGE (a)-[:TYPE {weight:1.0}]->(d)
MERGE (a)-[:TYPE {weight:-1.0}]->(e)
MERGE (a)-[:TYPE {weight:1.0}]->(f)
MERGE (a)-[:TYPE {weight:-1.0}]->(g)
MERGE (b)-[:TYPE {weight:-1.0}]->(c)
MERGE (c)-[:TYPE {weight:1.0}]->(d)
MERGE (d)-[:TYPE {weight:-1.0}]->(e)
MERGE (e)-[:TYPE {weight:1.0}]->(f)
MERGE (f)-[:TYPE {weight:-1.0}]->(g)
MERGE (g)-[:TYPE {weight:1.0}]->(b);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

call algo.balancedTriads.stream('Person','TYPE',{weightProperty:'weight'})
YIELD nodeId, balanced, unbalanced
RETURN algo.asNode(nodeId).name as person,balanced,unbalanced
ORDER BY balanced + unbalanced DESC
LIMIT 10

// end::stream-sample-graph[]


// tag::write-sample-graph[]

CALL algo.balancedTriads('Person', 'TYPE', {weightProperty:'weight'}) 
YIELD loadMillis, computeMillis, writeMillis, nodeCount, balancedTriadCount, unbalancedTriadCount;

// end::write-sample-graph[]