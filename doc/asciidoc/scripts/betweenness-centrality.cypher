// tag::create-sample-graph[]

MERGE (nAlice:User {id:'Alice'})
MERGE (nBridget:User {id:'Bridget'})
MERGE (nCharles:User {id:'Charles'})
MERGE (nDoug:User {id:'Doug'})
MERGE (nMark:User {id:'Mark'})
MERGE (nMichael:User {id:'Michael'})

MERGE (nAlice)-[:MANAGE]->(nBridget)
MERGE (nAlice)-[:MANAGE]->(nCharles)
MERGE (nAlice)-[:MANAGE]->(nDoug)
MERGE (nMark)-[:MANAGE]->(nAlice)
MERGE (nCharles)-[:MANAGE]->(nMichael);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.betweenness.stream('User','MANAGE',{direction:'out'}) 
YIELD nodeId, centrality

MATCH (user:User) WHERE id(user) = nodeId

RETURN user.id AS user,centrality
ORDER BY centrality DESC;

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.betweenness('User','MANAGE', {direction:'out',write:true, writeProperty:'centrality'}) 
YIELD nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis;

// end::write-sample-graph[]

// tag::cypher-loading[]

CALL algo.betweenness(
  'MATCH (p:User) RETURN id(p) as id',
  'MATCH (p1:User)-[:MANAGE]->(p2:User) RETURN id(p1) as source, id(p2) as target',
  {graph:'cypher', write: true}
);

// end::cypher-loading[]

// tag::stream-rabrandes-graph[]

CALL algo.betweenness.sampled.stream('User','MANAGE',
  {strategy:'random', probability:1.0, maxDepth:1, direction: "out"})

YIELD nodeId, centrality

MATCH (user) WHERE id(user) = nodeId
RETURN user.id AS user,centrality
ORDER BY centrality DESC;

// end::stream-rabrandes-graph[]

// tag::write-rabrandes-graph[]

CALL algo.betweenness.sampled('User','MANAGE',
  {strategy:'random', probability:1.0, writeProperty:'centrality', maxDepth:1, direction: "out"})
YIELD nodes, minCentrality, maxCentrality

// end::write-rabrandes-graph[]


CALL algo.<name>.stream('Label','TYPE',{conf})
YIELD nodeId, score

CALL algo.<name>('Label','TYPE',{conf})

CALL algo.<name>(
  'MATCH ... RETURN id(n)',
  'MATCH (n)-->(m)
   RETURN id(n) as source,
          id(m) as target',   {graph:'cypher'});


WITH "https://api.stackexchange.com/2.2/questions?pagesize=100&order=desc&sort=creation&tagged=neo4j&site=stackoverflow&filter=!5-i6Zw8Y)4W7vpy91PMYsKM-k9yzEsSC1_Uxlf" AS url
CALL apoc.load.json(url) YIELD value

UNWIND value.items AS q

MERGE (question:Question {id:q.question_id}) ON CREATE
  SET question.title = q.title, question.share_link = q.share_link, question.favorite_count = q.favorite_count

MERGE (owner:User {id:q.owner.user_id}) ON CREATE SET owner.display_name = q.owner.display_name
MERGE (owner)-[:ASKED]->(question)

FOREACH (tagName IN q.tags | MERGE (tag:Tag {name:tagName}) MERGE (question)-[:TAGGED]->(tag))
FOREACH (a IN q.answers |
   MERGE (question)<-[:ANSWERS]-(answer:Answer {id:a.answer_id})
   MERGE (answerer:User {id:a.owner.user_id}) ON CREATE SET answerer.display_name = a.owner.display_name
   MERGE (answer)<-[:PROVIDED]-(answerer)
);

WITH "jdbc:mysql://localhost:3306/northwind?user=root" AS url
CALL apoc.load.jdbc(url,"products") YIELD row
MERGE(p:Product {id: row.ProductID})
SET p.name = row.ProductName, p.unitPrice = row.UnitPrice;


CALL algo.pageRank.stream('Page', 'Link', {iterations:5}) YIELD nodeId, score
WITH * ORDER BY score DESC LIMIT 5
RETURN gds.util.asNode(nodeId).title, score;

call algo.unionFind.stream(
'match (o:output)-[:locked]->(a) with a limit 10000000
    return id(a) as id',

'match (o:output)-[:locked]->(a) with o,a limit 10000000
    match (o)-[:in]->(tx)-[:out]->(o2)-[:locked]->(a2)
    return id(a) as source, id(a2) as target, count(tx) as value',

{graph:'cypher'})
YIELD setId, nodeId
RETURN setId, count(*) as size
ORDER BY size DESC LIMIT 10;


