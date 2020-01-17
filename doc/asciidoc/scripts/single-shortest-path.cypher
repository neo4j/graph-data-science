// tag::single-pair-stream-sample-graph[]

MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})
CALL algo.shortestPath.stream(start, end, 'cost') 
YIELD nodeId, cost
RETURN gds.util.asNode(nodeId).name AS name, cost

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

RETURN gds.util.asNode(nodeId).name AS destination, distance

// end::delta-stream-sample-graph[]


// tag::delta-write-sample-graph[]

MATCH (n:Loc {name:'A'})
CALL algo.shortestPath.deltaStepping(n, 'cost', 3.0, {defaultValue:1.0, write:true, writeProperty:'sssp'})
YIELD nodeCount, loadDuration, evalDuration, writeDuration 
RETURN nodeCount, loadDuration, evalDuration, writeDuration

// end::delta-write-sample-graph[]

// tag::cypher-loading[]

MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})
CALL algo.shortestPath(start, end, 'cost',{
nodeQuery:'MATCH(n:Loc) WHERE not n.name = "c" RETURN id(n) as id',
relationshipQuery:'MATCH(n:Loc)-[r:ROAD]->(m:Loc) RETURN id(n) as source, id(m) as target, r.cost as weight',
graph:'cypher'}) 
YIELD writeMillis,loadMillis,nodeCount, totalCost
RETURN writeMillis,loadMillis,nodeCount,totalCost

// end::cypher-loading[]