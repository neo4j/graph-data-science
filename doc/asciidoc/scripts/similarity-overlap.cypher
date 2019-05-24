// tag::function[]
RETURN algo.similarity.overlap([1,2,3], [1,2,4,5]) AS similarity
// end::function[]

// tag::create-sample-graph[]

MERGE (fahrenheit451:Book {title:'Fahrenheit 451'})
MERGE (dune:Book {title:'Dune'})
MERGE (hungerGames:Book {title:'The Hunger Games'})
MERGE (nineteen84:Book {title:'1984'})
MERGE (gatsby:Book {title:'The Great Gatsby'})

MERGE (scienceFiction:Genre {name: "Science Fiction"})
MERGE (fantasy:Genre {name: "Fantasy"})
MERGE (dystopia:Genre {name: "Dystopia"})
MERGE (classics:Genre {name: "Classics"})

MERGE (fahrenheit451)-[:HAS_GENRE]->(dystopia)
MERGE (fahrenheit451)-[:HAS_GENRE]->(scienceFiction)
MERGE (fahrenheit451)-[:HAS_GENRE]->(fantasy)
MERGE (fahrenheit451)-[:HAS_GENRE]->(classics)

MERGE (hungerGames)-[:HAS_GENRE]->(scienceFiction)
MERGE (hungerGames)-[:HAS_GENRE]->(fantasy)
MERGE (hungerGames)-[:HAS_GENRE]->(romance)

MERGE (nineteen84)-[:HAS_GENRE]->(scienceFiction)
MERGE (nineteen84)-[:HAS_GENRE]->(dystopia)
MERGE (nineteen84)-[:HAS_GENRE]->(classics)

MERGE (dune)-[:HAS_GENRE]->(scienceFiction)
MERGE (dune)-[:HAS_GENRE]->(fantasy)
MERGE (dune)-[:HAS_GENRE]->(classics)

MERGE (gatsby)-[:HAS_GENRE]->(classics)

// end::create-sample-graph[]

// tag::stream[]
MATCH (book:Book)-[:HAS_GENRE]->(genre)
WITH {item:id(genre), categories: collect(id(book))} as userData
WITH collect(userData) as data
CALL algo.similarity.overlap.stream(data)
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to,
       count1, count2, intersection, similarity
ORDER BY similarity DESC
// end::stream[]

// tag::stream-similarity-cutoff[]
MATCH (book:Book)-[:HAS_GENRE]->(genre)
WITH {item:id(genre), categories: collect(id(book))} as userData
WITH collect(userData) as data
CALL algo.similarity.overlap.stream(data, {similarityCutoff: 0.75})
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to,
       count1, count2, intersection, similarity
ORDER BY similarity DESC
// end::stream-similarity-cutoff[]

// tag::stream-topk[]
MATCH (book:Book)-[:HAS_GENRE]->(genre)
WITH {item:id(genre), categories: collect(id(book))} as userData
WITH collect(userData) as data
CALL algo.similarity.overlap.stream(data, {topK: 2})
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to,
       count1, count2, intersection, similarity
ORDER BY from
// end::stream-topk[]

// tag::write-back[]
MATCH (book:Book)-[:HAS_GENRE]->(genre)
WITH {item:id(genre), categories: collect(id(book))} as userData
WITH collect(userData) as data
CALL algo.similarity.overlap(data, {topK: 2, similarityCutoff: 0.5, write:true})
YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100
RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95
// end::write-back[]

// tag::query[]
MATCH path = (fantasy:Genre {name: "Fantasy"})-[:NARROWER_THAN*]->(genre)
RETURN [node in nodes(path) | node.name] AS hierarchy
ORDER BY length(path)
// end::query[]


// tag::source-target-ids[]
MATCH (book:Book)-[:HAS_GENRE]->(genre)
WITH {item:id(genre), name: genre.name, categories: collect(id(book))} as userData
WITH collect(userData) as data

// create sourceIds list containing ids for Fantasy and Classics
WITH data,
     [value in data WHERE value.name IN ["Fantasy", "Classics"] | value.item ] AS sourceIds

CALL algo.similarity.overlap.stream(data, {sourceIds: sourceIds})
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.getNodeById(item1).name AS from, algo.getNodeById(item2).name AS to, similarity
ORDER BY similarity DESC
// end::source-target-ids[]
