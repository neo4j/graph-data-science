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