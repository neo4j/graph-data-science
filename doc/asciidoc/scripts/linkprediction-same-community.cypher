// tag::create-sample-graph[]

MERGE (zhen:Person {name: "Zhen"}) SET zhen.community = 1
MERGE (praveena:Person {name: "Praveena"}) SET praveena.community=2
MERGE (michael:Person {name: "Michael"}) SET michael.community = 1

MERGE (arya:Person {name: "Arya"}) SET arya.partition = 5
MERGE (karin:Person {name: "Karin"}) SET karin.partition = 5

MERGE (jennifer:Person {name: "Jennifer"})

// end::create-sample-graph[]

// tag::same-community[]
MATCH (p1:Person {name: 'Michael'})
MATCH (p2:Person {name: 'Zhen'})
RETURN gds.alpha.linkprediction.sameCommunity(p1, p2) AS score
// end::same-community[]

// tag::different-community[]
MATCH (p1:Person {name: 'Michael'})
MATCH (p2:Person {name: 'Praveena'})
RETURN gds.alpha.linkprediction.sameCommunity(p1, p2) AS score
// end::different-community[]

// tag::missing-community[]
MATCH (p1:Person {name: 'Michael'})
MATCH (p2:Person {name: 'Jennifer'})
RETURN gds.alpha.linkprediction.sameCommunity(p1, p2) AS score
// end::missing-community[]

// tag::same-community-specific[]
MATCH (p1:Person {name: 'Arya'})
MATCH (p2:Person {name: 'Karin'})
RETURN gds.alpha.linkprediction.sameCommunity(p1, p2, 'partition') AS score
// end::same-community-specific[]
