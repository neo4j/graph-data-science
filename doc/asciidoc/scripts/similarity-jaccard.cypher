// tag::function[]
RETURN algo.similarity.jaccard([1,2,3], [1,2,4,5]) AS similarity
// end::function[]

// tag::create-sample-graph[]

MERGE (french:Cuisine {name:'French'})
MERGE (italian:Cuisine {name:'Italian'})
MERGE (indian:Cuisine {name:'Indian'})
MERGE (lebanese:Cuisine {name:'Lebanese'})
MERGE (portuguese:Cuisine {name:'Portuguese'})

MERGE (zhen:Person {name: "Zhen"})
MERGE (praveena:Person {name: "Praveena"})
MERGE (michael:Person {name: "Michael"})
MERGE (arya:Person {name: "Arya"})
MERGE (karin:Person {name: "Karin"})

MERGE (praveena)-[:LIKES]->(indian)
MERGE (praveena)-[:LIKES]->(portuguese)

MERGE (zhen)-[:LIKES]->(french)
MERGE (zhen)-[:LIKES]->(indian)

MERGE (michael)-[:LIKES]->(french)
MERGE (michael)-[:LIKES]->(italian)
MERGE (michael)-[:LIKES]->(indian)

MERGE (arya)-[:LIKES]->(lebanese)
MERGE (arya)-[:LIKES]->(italian)
MERGE (arya)-[:LIKES]->(portuguese)

MERGE (karin)-[:LIKES]->(lebanese)
MERGE (karin)-[:LIKES]->(italian)

// end::create-sample-graph[]

// tag::function-cypher[]
MATCH (p1:Person {name: 'Karin'})-[:LIKES]->(cuisine1)
WITH p1, collect(id(cuisine1)) AS p1Cuisine
MATCH (p2:Person {name: "Arya"})-[:LIKES]->(cuisine2)
WITH p1, p1Cuisine, p2, collect(id(cuisine2)) AS p2Cuisine
RETURN p1.name AS from,
       p2.name AS to,
       algo.similarity.jaccard(p1Cuisine, p2Cuisine) AS similarity
// end::function-cypher[]

// tag::function-cypher-all[]
MATCH (p1:Person {name: 'Karin'})-[:LIKES]->(cuisine1)
WITH p1, collect(id(cuisine1)) AS p1Cuisine
MATCH (p2:Person)-[:LIKES]->(cuisine2) WHERE p1 <> p2
WITH p1, p1Cuisine, p2, collect(id(cuisine2)) AS p2Cuisine
RETURN p1.name AS from,
       p2.name AS to,
       algo.similarity.jaccard(p1Cuisine, p2Cuisine) AS similarity
ORDER BY similarity DESC
// end::function-cypher-all[]
