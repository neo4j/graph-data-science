// tag::import-nodes[]
LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS row
MERGE (:Page {name: row.page})
// end::import-nodes[]

// tag::import-relationships[]
LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row
MATCH (p1:Page {name: row.source})
MATCH (p2:Page {name: row.target})
MERGE (p1)-[:LINKS]->(p2)
// end::import-relationships[]

// tag::algorithm[]
CALL algo.pageRank.stream('Page', 'LINKS', {iterations:20, dampingFactor:0.85})
YIELD nodeId, score
RETURN algo.asNode(nodeId).name,score
ORDER BY score DESC
// end::algorithm[]

// tag::result[]
.Results
[opts="header",cols="1,1"]
|===
| name | pageRank
| Home | 3.232
| Product | 1.059
| Links | 1.059
| About | 1.059
| Site A | 0.328
| Site B | 0.328
| Site C | 0.328
| Site D | 0.328
|===
// end::result[]
