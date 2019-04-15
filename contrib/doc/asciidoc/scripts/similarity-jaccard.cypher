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


// tag::create-sample-graph-procedure[]

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

MERGE (shrimp:Recipe {title: "Shrimp Bolognese"})
MERGE (saltimbocca:Recipe {title: "Saltimbocca alla roman"})
MERGE (periperi:Recipe {title: "Peri Peri Naan"})

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

MERGE (shrimp)-[:TYPE]->(italian)
MERGE (shrimp)-[:TYPE]->(indian)

MERGE (saltimbocca)-[:TYPE]->(italian)
MERGE (saltimbocca)-[:TYPE]->(french)

MERGE (periperi)-[:TYPE]->(portuguese)
MERGE (periperi)-[:TYPE]->(indian)

// end::create-sample-graph-procedure[]


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

// tag::stream[]
MATCH (p:Person)-[:LIKES]->(cuisine)
WITH {item:id(p), categories: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard.stream(data)
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, intersection, similarity
ORDER BY similarity DESC
// end::stream[]

// tag::stream-similarity-cutoff[]
MATCH (p:Person)-[:LIKES]->(cuisine)
WITH {item:id(p), categories: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard.stream(data, {similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, intersection, similarity
ORDER BY similarity DESC
// end::stream-similarity-cutoff[]

// tag::stream-topk[]
MATCH (p:Person)-[:LIKES]->(cuisine)
WITH {item:id(p), categories: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard.stream(data, {topK: 1, similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY from
// end::stream-topk[]

// tag::write-back[]
MATCH (p:Person)-[:LIKES]->(cuisine)
WITH {item:id(p), categories: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard(data, {topK: 1, similarityCutoff: 0.1, write:true})
YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100
RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95
// end::write-back[]

// tag::query[]
MATCH (p:Person {name: "Praveena"})-[:SIMILAR]->(other),
      (other)-[:LIKES]->(cuisine)
WHERE not((p)-[:LIKES]->(cuisine))
RETURN cuisine.name AS cuisine
// end::query[]

// tag::source-target-ids[]
// compute categories for recipes
MATCH (recipe:Recipe)-[:TYPE]->(cuisine)
WITH {item:id(recipe), categories: collect(id(cuisine))} as data
WITH collect(data) AS recipeCuisines

// compute categories for people
MATCH (person:Person)-[:LIKES]->(cuisine)
WITH recipeCuisines, {item:id(person), categories: collect(id(cuisine))} as data
WITH recipeCuisines, collect(data) AS personCuisines

// create sourceIds and targetIds lists
WITH recipeCuisines, personCuisines,
     [value in recipeCuisines | value.item] AS sourceIds,
     [value in personCuisines | value.item] AS targetIds

CALL algo.similarity.jaccard.stream(recipeCuisines + personCuisines, {sourceIds: sourceIds, targetIds: targetIds})
YIELD item1, item2, similarity
WITH algo.getNodeById(item1) AS from, algo.getNodeById(item2) AS to, similarity
RETURN from.title AS from, to.name AS to, similarity
ORDER BY similarity DESC
LIMIT 10
// end::source-target-ids[]

// tag::source-target-ids-2[]
MATCH (person:Person)-[:LIKES]->(cuisine)
WITH {item:id(person), name: person.name, categories: collect(id(cuisine))} as data
WITH collect(data) AS personCuisines

// create sourceIds list containing ids for Praveena and Arya
WITH personCuisines,
     [value in personCuisines WHERE value.name IN ["Praveena", "Arya"] | value.item ] AS sourceIds

CALL algo.similarity.jaccard.stream(personCuisines, {sourceIds: sourceIds, topK: 1})
YIELD item1, item2, similarity
WITH algo.getNodeById(item1) AS from, algo.getNodeById(item2) AS to, similarity
RETURN from.name AS from, to.name AS to, similarity
ORDER BY similarity DESC
// end::source-target-ids-2[]