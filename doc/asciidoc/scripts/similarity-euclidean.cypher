// tag::function[]
RETURN algo.similarity.euclideanDistance([3,8,7,5,2,9], [10,8,6,6,4,5]) AS similarity
// end::function[]

// tag::create-sample-graph[]

MERGE (french:Cuisine {name:'French'})
MERGE (italian:Cuisine {name:'Italian'})
MERGE (indian:Cuisine {name:'Indian'})
MERGE (lebanese:Cuisine {name:'Lebanese'})
MERGE (portuguese:Cuisine {name:'Portuguese'})
MERGE (british:Cuisine {name:'British'})
MERGE (mauritian:Cuisine {name:'Mauritian'})

MERGE (zhen:Person {name: "Zhen"})
MERGE (praveena:Person {name: "Praveena"})
MERGE (michael:Person {name: "Michael"})
MERGE (arya:Person {name: "Arya"})
MERGE (karin:Person {name: "Karin"})

MERGE (praveena)-[:LIKES {score: 9}]->(indian)
MERGE (praveena)-[:LIKES {score: 7}]->(portuguese)
MERGE (praveena)-[:LIKES {score: 8}]->(british)
MERGE (praveena)-[:LIKES {score: 1}]->(mauritian)

MERGE (zhen)-[:LIKES {score: 10}]->(french)
MERGE (zhen)-[:LIKES {score: 6}]->(indian)
MERGE (zhen)-[:LIKES {score: 2}]->(british)

MERGE (michael)-[:LIKES {score: 8}]->(french)
MERGE (michael)-[:LIKES {score: 7}]->(italian)
MERGE (michael)-[:LIKES {score: 9}]->(indian)
MERGE (michael)-[:LIKES {score: 3}]->(portuguese)

MERGE (arya)-[:LIKES {score: 10}]->(lebanese)
MERGE (arya)-[:LIKES {score: 10}]->(italian)
MERGE (arya)-[:LIKES {score: 7}]->(portuguese)
MERGE (arya)-[:LIKES {score: 9}]->(mauritian)

MERGE (karin)-[:LIKES {score: 9}]->(lebanese)
MERGE (karin)-[:LIKES {score: 7}]->(italian)
MERGE (karin)-[:LIKES {score: 10}]->(portuguese)

// end::create-sample-graph[]

// tag::function-cypher[]
MATCH (p1:Person {name: 'Zhen'})-[likes1:LIKES]->(cuisine)
MATCH (p2:Person {name: "Praveena"})-[likes2:LIKES]->(cuisine)
RETURN p1.name AS from,
       p2.name AS to,
       algo.similarity.euclideanDistance(collect(likes1.score), collect(likes2.score)) AS similarity
// end::function-cypher[]

// tag::function-cypher-all[]
MATCH (p1:Person {name: 'Zhen'})-[likes1:LIKES]->(cuisine)
MATCH (p2:Person)-[likes2:LIKES]->(cuisine) WHERE p2 <> p1
RETURN p1.name AS from,
       p2.name AS to,
       algo.similarity.euclideanDistance(collect(likes1.score), collect(likes2.score)) AS similarity
ORDER BY similarity DESC
// end::function-cypher-all[]

// tag::stream[]
MATCH (p:Person), (c:Cuisine)
OPTIONAL MATCH (p)-[likes:LIKES]->(c)
WITH {item:id(p), weights: collect(coalesce(likes.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.euclidean.stream(data)
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity
// end::stream[]

// tag::stream-finite[]
MATCH (p:Person), (c:Cuisine)
OPTIONAL MATCH (p)-[likes:LIKES]->(c)
WITH {item:id(p), weights: collect(coalesce(likes.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.euclidean.stream(data)
YIELD item1, item2, count1, count2, similarity
WHERE algo.isFinite(similarity)
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity
// end::stream-finite[]

// tag::stream-similarity-cutoff[]
MATCH (p:Person), (c:Cuisine)
OPTIONAL MATCH (p)-[likes:LIKES]->(c)
WITH {item:id(p), weights: collect(coalesce(likes.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.euclidean.stream(data, {similarityCutoff: 4.0})
YIELD item1, item2, count1, count2, similarity
WHERE algo.isFinite(similarity)
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity
// end::stream-similarity-cutoff[]

// tag::stream-topk[]
MATCH (p:Person), (c:Cuisine)
OPTIONAL MATCH (p)-[likes:LIKES]->(c)
WITH {item:id(p), weights: collect(coalesce(likes.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.euclidean.stream(data, {topK:1})
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY from
// end::stream-topk[]

// tag::write-back[]
MATCH (p:Person), (c:Cuisine)
OPTIONAL MATCH (p)-[likes:LIKES]->(c)
WITH {item:id(p), weights: collect(coalesce(likes.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.euclidean(data, {topK: 1, write:true})
YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100
RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95
// end::write-back[]

// tag::query[]
MATCH (p:Person {name: "Praveena"})-[:SIMILAR]->(other),
      (other)-[:LIKES]->(cuisine)
WHERE not((p)-[:LIKES]->(cuisine))
RETURN cuisine.name AS cuisine
// end::query[]

// tag::cypher-projection[]
WITH "MATCH (person:Person)-[likes:LIKES]->(c)
      RETURN id(person) AS item, id(c) AS category, likes.score AS weight" AS query
CALL algo.similarity.euclidean(query, {
  graph: 'cypher', topK: 1, similarityCutoff: 4.0, write:true
})
YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p95
RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95
// end::cypher-projection[]


// tag::create-sample-embedding-graph[]

MERGE (french:Cuisine {name:'French'})          SET french.embedding = [0.71, 0.33, 0.81, 0.52, 0.41]
MERGE (italian:Cuisine {name:'Italian'})        SET italian.embedding = [0.31, 0.72, 0.58, 0.67, 0.31]
MERGE (indian:Cuisine {name:'Indian'})          SET indian.embedding = [0.43, 0.26, 0.98, 0.51, 0.76]
MERGE (lebanese:Cuisine {name:'Lebanese'})      SET lebanese.embedding = [0.12, 0.23, 0.35, 0.31, 0.39]
MERGE (portuguese:Cuisine {name:'Portuguese'})  SET portuguese.embedding = [0.47, 0.98, 0.81, 0.72, 0.89]
MERGE (british:Cuisine {name:'British'})        SET british.embedding = [0.94, 0.12, 0.23, 0.4, 0.71]
MERGE (mauritian:Cuisine {name:'Mauritian'})    SET mauritian.embedding = [0.31, 0.56, 0.98, 0.21, 0.62]

// end::create-sample-embedding-graph[]

// tag::embedding-graph-stream[]

MATCH (c:Cuisine)
WITH {item:id(c), weights: c.embedding} as userData
WITH collect(userData) as data
CALL algo.similarity.euclidean.stream(data, {skipValue: null})
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity DESC

// end::embedding-graph-stream[]

// tag::source-target-ids[]
MATCH (p:Person), (c:Cuisine)
OPTIONAL MATCH (p)-[likes:LIKES]->(c)
WITH {item:id(p), name: p.name, weights: collect(coalesce(likes.score, algo.NaN()))} as userData
WITH collect(userData) as personCuisines

// create sourceIds list containing ids for Praveena and Arya
WITH personCuisines,
     [value in personCuisines WHERE value.name IN ["Praveena", "Arya"] | value.item ] AS sourceIds

CALL algo.similarity.euclidean.stream(personCuisines, {sourceIds: sourceIds, topK: 1})
YIELD item1, item2, similarity
WITH algo.getNodeById(item1) AS from, algo.getNodeById(item2) AS to, similarity
RETURN from.name AS from, to.name AS to, similarity
ORDER BY similarity DESC
// end::source-target-ids[]