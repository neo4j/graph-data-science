// tag::create-sample-graph[]

MERGE (a:Loc {name:'A'})
MERGE (b:Loc {name:'B'})
MERGE (c:Loc {name:'C'})
MERGE (d:Loc {name:'D'})
MERGE (e:Loc {name:'E'})
MERGE (f:Loc {name:'F'})

MERGE (a)-[:ROAD {cost:50}]->(b)
MERGE (a)-[:ROAD {cost:50}]->(c)
MERGE (a)-[:ROAD {cost:100}]->(d)
MERGE (b)-[:ROAD {cost:40}]->(d)
MERGE (c)-[:ROAD {cost:40}]->(d)
MERGE (c)-[:ROAD {cost:80}]->(e)
MERGE (d)-[:ROAD {cost:30}]->(e)
MERGE (d)-[:ROAD {cost:80}]->(f)
MERGE (e)-[:ROAD {cost:40}]->(f);

// end::create-sample-graph[]


// tag::single-pair-stream-sample-graph[]

MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})
CALL algo.shortestPath.stream(start, end, 'cost') 
YIELD nodeId, cost
RETURN algo.asNode(nodeId).name AS name, cost

// end::single-pair-stream-sample-graph[]


// tag::single-pair-write-sample-graph[]

MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})
CALL algo.shortestPath(start, end, 'cost',{write:true,writeProperty:'sssp'}) 
YIELD writeMillis,loadMillis,nodeCount, totalCost
RETURN writeMillis,loadMillis,nodeCount,totalCost

// end::single-pair-write-sample-graph[]


// tag::delta-stream-sample-graph[]

MATCH (n:Loc {name:'A'})
CALL algo.shortestPath.deltaStepping.stream(n, 'cost', 3.0)
YIELD nodeId, distance

RETURN algo.asNode(nodeId).name AS destination, distance

// end::delta-stream-sample-graph[]


// tag::delta-write-sample-graph[]

MATCH (n:Loc {name:'A'})
CALL algo.shortestPath.deltaStepping(n, 'cost', 3.0, {defaultValue:1.0, write:true, writeProperty:'sssp'})
YIELD nodeCount, loadDuration, evalDuration, writeDuration 
RETURN nodeCount, loadDuration, evalDuration, writeDuration

// end::delta-write-sample-graph[]


// tag::all-pairs-sample-graph[]

CALL algo.allShortestPaths.stream('cost',{nodeQuery:'Loc',defaultValue:1.0})
YIELD sourceNodeId, targetNodeId, distance
WITH sourceNodeId, targetNodeId, distance 
WHERE algo.isFinite(distance) = true

MATCH (source:Loc) WHERE id(source) = sourceNodeId
MATCH (target:Loc) WHERE id(target) = targetNodeId
WITH source, target, distance WHERE source <> target

RETURN source.name AS source, target.name AS target, distance
ORDER BY distance DESC
LIMIT 10

// end::all-pairs-sample-graph[]


// tag::all-pairs-bidirected-graph[]

CALL algo.allShortestPaths.stream('cost', {
nodeQuery:'MATCH (n:Loc) RETURN id(n) as id', 
relationshipQuery:'MATCH (n:Loc)-[r]-(p:Loc) RETURN id(n) as source, id(p) as target, r.cost as weight',
graph:'cypher', defaultValue:1.0})


// end::all-pairs-bidirected-graph[]


// tag::cypher-loading[]

MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})
CALL algo.shortestPath(start, end, 'cost',{
nodeQuery:'MATCH(n:Loc) WHERE not n.name = "c" RETURN id(n) as id',
relationshipQuery:'MATCH(n:Loc)-[r:ROAD]->(m:Loc) RETURN id(n) as source, id(m) as target, r.cost as weight',
graph:'cypher'}) 
YIELD writeMillis,loadMillis,nodeCount, totalCost
RETURN writeMillis,loadMillis,nodeCount,totalCost

// end::cypher-loading[]

// tag::all-pairs-huge-projection[]

CALL algo.allShortestPaths.stream('cost',{nodeQuery:'Loc',defaultValue:1.0,graph:'huge'})
YIELD sourceNodeId, targetNodeId, distance
RETURN sourceNodeId, targetNodeId, distance LIMIT 10

// end::all-pairs-huge-projection[]
