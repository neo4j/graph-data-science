// tag::create-sample-graph[]

MERGE (a:Station{name:"King's Cross St. Pancras"})
SET a.latitude = 51.5308,a.longitude = -0.1238
MERGE (b:Station{name:"Euston"})
SET b.latitude = 51.5282, b.longitude = -0.1337
MERGE (c:Station{name:"Camden Town"})
SET c.latitude = 51.5392, c.longitude = -0.1426
MERGE (d:Station{name:"Mornington Crescent"})
SET d.latitude = 51.5342, d.longitude = -0.1387
MERGE (e:Station{name:"Kentish Town"})
SET e.latitude = 51.5507, e.longitude = -0.1402
MERGE (a)-[:CONNECTION{time:2}]->(b)
MERGE (b)-[:CONNECTION{time:3}]->(c)
MERGE (b)-[:CONNECTION{time:2}]->(d)
MERGE (d)-[:CONNECTION{time:2}]->(c)
MERGE (c)-[:CONNECTION{time:2}]->(e);

// end::create-sample-graph[]


// tag::stream-sample-graph[]

MATCH (start:Station{name:"King's Cross St. Pancras"}),(end:Station{name:"Kentish Town"})
CALL algo.shortestPath.astar.stream(start, end, 'time', 'latitude', 'longitude', {defaultValue:1.0})
YIELD nodeId, cost
RETURN gds.util.asNode(nodeId).name as station,cost

// end::stream-sample-graph[]

// tag::cypher-loading[]

MATCH (start:Station{name:"King's Cross St. Pancras"}),(end:Station{name:"Kentish Town"})
CALL algo.shortestPath.astar.stream(start, end, 'time','latitude','longitude',{
nodeQuery:'MATCH (p:Station) RETURN id(p) as id',
relationshipQuery:'MATCH (p1:Station)-[r:CONNECTION]->(p2:Station) RETURN id(p1) as source, id(p2) as target,r.time as weight',
graph:'cypher'}) 
YIELD nodeId, cost
RETURN nodeId,cost

// end::cypher-loading[]
