// tag::basic[]
RETURN gds.alpha.ml.oneHotEncoding(["Chinese", "Indian", "Italian"], ["Italian"]) AS vector
// end::basic[]

// tag::create-sample-graph[]

MERGE (french:Cuisine {name:'French'})
MERGE (italian:Cuisine {name:'Italian'})
MERGE (indian:Cuisine {name:'Indian'})

MERGE (zhen:Person {name: "Zhen"})
MERGE (praveena:Person {name: "Praveena"})
MERGE (michael:Person {name: "Michael"})
MERGE (arya:Person {name: "Arya"})

MERGE (praveena)-[:LIKES]->(indian)
MERGE (zhen)-[:LIKES]->(french)
MERGE (michael)-[:LIKES]->(french)
MERGE (michael)-[:LIKES]->(italian);

// end::create-sample-graph[]

// tag::one-hot-encoding-query[]
MATCH (cuisine:Cuisine)
WITH cuisine ORDER BY cuisine.name
WITH collect(cuisine) AS cuisines
MATCH (p:Person)
RETURN p.name AS person,
       gds.alpha.ml.oneHotEncoding(cuisines, [(p)-[:LIKES]->(cuisine) | cuisine]) AS encoding
ORDER BY person
// end::one-hot-encoding-query[]
