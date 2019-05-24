// tag::function[]
RETURN algo.similarity.pearson([5,8,7,5,4,9], [7,8,6,6,4,5]) AS similarity
// end::function[]


// tag::create-sample-graph[]

MERGE (home_alone:Movie {name:'Home Alone'})
MERGE (matrix:Movie {name:'The Matrix'})
MERGE (good_men:Movie {name:'A Few Good Men'})
MERGE (top_gun:Movie {name:'Top Gun'})
MERGE (jerry:Movie {name:'Jerry Maguire'})
MERGE (gruffalo:Movie {name:'The Gruffalo'})

MERGE (zhen:Person {name: "Zhen"})
MERGE (praveena:Person {name: "Praveena"})
MERGE (michael:Person {name: "Michael"})
MERGE (arya:Person {name: "Arya"})
MERGE (karin:Person {name: "Karin"})

MERGE (zhen)-[:RATED {score: 2}]->(home_alone)
MERGE (zhen)-[:RATED {score: 2}]->(good_men)
MERGE (zhen)-[:RATED {score: 3}]->(matrix)
MERGE (zhen)-[:RATED {score: 6}]->(jerry)

MERGE (praveena)-[:RATED {score: 6}]->(home_alone)
MERGE (praveena)-[:RATED {score: 7}]->(good_men)
MERGE (praveena)-[:RATED {score: 8}]->(matrix)
MERGE (praveena)-[:RATED {score: 9}]->(jerry)

MERGE (michael)-[:RATED {score: 7}]->(home_alone)
MERGE (michael)-[:RATED {score: 9}]->(good_men)
MERGE (michael)-[:RATED {score: 3}]->(jerry)
MERGE (michael)-[:RATED {score: 4}]->(top_gun)

MERGE (arya)-[:RATED {score: 8}]->(top_gun)
MERGE (arya)-[:RATED {score: 1}]->(matrix)
MERGE (arya)-[:RATED {score: 10}]->(jerry)
MERGE (arya)-[:RATED {score: 10}]->(gruffalo)

MERGE (karin)-[:RATED {score: 9}]->(top_gun)
MERGE (karin)-[:RATED {score: 7}]->(matrix)
MERGE (karin)-[:RATED {score: 7}]->(home_alone)
MERGE (karin)-[:RATED {score: 9}]->(gruffalo)

// end::create-sample-graph[]

// tag::function-cypher[]
MATCH (p1:Person {name: 'Arya'})-[rated:RATED]->(movie)
WITH p1, algo.similarity.asVector(movie, rated.score) AS p1Vector
MATCH (p2:Person {name: 'Karin'})-[rated:RATED]->(movie)
WITH p1, p2, p1Vector, algo.similarity.asVector(movie, rated.score) AS p2Vector
RETURN p1.name AS from,
       p2.name AS to,
       algo.similarity.pearson(p1Vector, p2Vector, {vectorType: "maps"}) AS similarity
// end::function-cypher[]

// tag::function-cypher-all[]
MATCH (p1:Person {name: 'Arya'})-[rated:RATED]->(movie)
WITH p1, algo.similarity.asVector(movie, rated.score) AS p1Vector
MATCH (p2:Person)-[rated:RATED]->(movie) WHERE p2 <> p1
WITH p1, p2, p1Vector, algo.similarity.asVector(movie, rated.score) AS p2Vector
RETURN p1.name AS from,
       p2.name AS to,
       algo.similarity.pearson(p1Vector, p2Vector, {vectorType: "maps"}) AS similarity
ORDER BY similarity DESC
// end::function-cypher-all[]


// tag::stream[]
MATCH (p:Person), (m:Movie)
OPTIONAL MATCH (p)-[rated:RATED]->(m)
WITH {item:id(p), weights: collect(coalesce(rated.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.pearson.stream(data)
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity DESC
// end::stream[]

// tag::stream-similarity-cutoff[]
MATCH (p:Person), (m:Movie)
OPTIONAL MATCH (p)-[rated:RATED]->(m)
WITH {item:id(p), weights: collect(coalesce(rated.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.pearson.stream(data, {similarityCutoff: 0.1})
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity DESC
// end::stream-similarity-cutoff[]

// tag::stream-topk[]
MATCH (p:Person), (m:Movie)
OPTIONAL MATCH (p)-[rated:RATED]->(m)
WITH {item:id(p), weights: collect(coalesce(rated.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.pearson.stream(data, {topK:1, similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity DESC
// end::stream-topk[]

// tag::write-back[]
MATCH (p:Person), (m:Movie)
OPTIONAL MATCH (p)-[rated:RATED]->(m)
WITH {item:id(p), weights: collect(coalesce(rated.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.pearson(data, {topK: 1, similarityCutoff: 0.1, write:true})
YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100
RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95
// end::write-back[]

// tag::query[]
MATCH (p:Person {name: "Praveena"})-[:SIMILAR]->(other),
      (other)-[r:RATED]->(movie)
WHERE not((p)-[:RATED]->(movie)) and r.score >= 8
RETURN movie.name AS movie
// end::query[]


// tag::cypher-projection[]
WITH "MATCH (person:Person)-[rated:RATED]->(c)
      RETURN id(person) AS item, id(c) AS category, rated.score AS weight" AS query
CALL algo.similarity.pearson(query, {
  graph: 'cypher', topK: 1, similarityCutoff: 0.1, write:true
})
YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p95
RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95
// end::cypher-projection[]

// tag::create-sample-embedding-graph[]

MERGE (home_alone:Movie {name:'Home Alone'})    SET home_alone.embedding = [0.71, 0.33, 0.81, 0.52, 0.41]
MERGE (matrix:Movie {name:'The Matrix'})        SET matrix.embedding = [0.31, 0.72, 0.58, 0.67, 0.31]
MERGE (good_men:Movie {name:'A Few Good Men'})  SET good_men.embedding = [0.43, 0.26, 0.98, 0.51, 0.76]
MERGE (top_gun:Movie {name:'Top Gun'})          SET top_gun.embedding = [0.12, 0.23, 0.35, 0.31, 0.3]
MERGE (jerry:Movie {name:'Jerry Maguire'})      SET jerry.embedding = [0.47, 0.98, 0.81, 0.72, 0]

// end::create-sample-embedding-graph[]

// tag::embedding-graph-stream[]

MATCH (m:Movie)
WITH {item:id(m), weights: m.embedding} as userData
WITH collect(userData) as data
CALL algo.similarity.pearson.stream(data, {skipValue: null})
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity DESC

// end::embedding-graph-stream[]

// tag::source-target-ids[]
MATCH (p:Person), (m:Movie)
OPTIONAL MATCH (p)-[rated:RATED]->(m)
WITH {item:id(p), name: p.name, weights: collect(coalesce(rated.score, algo.NaN()))} as userData
WITH collect(userData) as personCuisines

// create sourceIds list containing ids for Praveena and Arya
WITH personCuisines,
     [value in personCuisines WHERE value.name IN ["Praveena", "Arya"] | value.item ] AS sourceIds

CALL algo.similarity.pearson.stream(personCuisines, {sourceIds: sourceIds, topK: 1})
YIELD item1, item2, similarity
WITH algo.getNodeById(item1) AS from, algo.getNodeById(item2) AS to, similarity
RETURN from.name AS from, to.name AS to, similarity
ORDER BY similarity DESC
// end::source-target-ids[]