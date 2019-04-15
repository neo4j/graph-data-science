// tag::load-schema[]
CALL apoc.schema.assert(
{Category:['name']},
{Business:['id'],User:['id'],Review:['id']});
// end::load-schema[]

// tag::load-business[]
CALL apoc.periodic.iterate("
CALL apoc.load.json('file:///dataset/business.json') YIELD value RETURN value
","
MERGE (b:Business{id:value.business_id})
SET b += apoc.map.clean(value, ['attributes','hours','business_id','categories','address','postal_code'],[]) 
WITH b,value.categories as categories
UNWIND categories as category
MERGE (c:Category{id:category})
MERGE (b)-[:IN_CATEGORY]->(c)
",{batchSize: 10000, iterateList: true});
// end::load-business[]

// tag::load-tip[]

CALL apoc.periodic.iterate("
CALL apoc.load.json('file:///dataset/tip.json') YIELD value RETURN value
","
MATCH (b:Business{id:value.business_id})
MERGE (u:User{id:value.user_id})
MERGE (u)-[:TIP{date:value.date,likes:value.likes}]->(b)
",{batchSize: 20000, iterateList: true});
// end::load-tip[]

// tag::load-review[]
CALL apoc.periodic.iterate("
CALL apoc.load.json('file:///dataset/review.json') 
YIELD value RETURN value
","
MERGE (b:Business{id:value.business_id})
MERGE (u:User{id:value.user_id})
MERGE (r:Review{id:value.review_id})
MERGE (u)-[:WROTE]->(r)
MERGE (r)-[:REVIEWS]->(b)
SET r += apoc.map.clean(value, ['business_id','user_id','review_id','text'],[0]) 
",{batchSize: 10000, iterateList: true});
// end::load-review[]


// tag::load-user[]

CALL apoc.periodic.iterate("
CALL apoc.load.json('file:///dataset/user.json') 
YIELD value RETURN value
","
MERGE (u:User{id:value.user_id})
SET u += apoc.map.clean(value, ['friends','user_id'],[0])
WITH u,value.friends as friends
UNWIND friends as friend
MERGE (u1:User{id:friend})
MERGE (u)-[:FRIEND]-(u1)
",{batchSize: 100, iterateList: true});
// end::load-user[]

// tag::social-network-local-props[]


MATCH (u:User)
RETURN avg(apoc.node.degree(u,'FRIEND')) as average_friends,
       stdev(apoc.node.degree(u,'FRIEND')) as stdev_friends,
       max(apoc.node.degree(u,'FRIEND')) as max_friends,
       min(apoc.node.degree(u,'FRIEND')) as min_friends

// end::social-network-local-props[]


// tag::reviewsimilarity-graph[]

CALL apoc.periodic.iterate(
"MATCH (p1:User) WHERE size((p1)-[:WROTE]->()) > 5 RETURN p1",
"
MATCH (p1)-[:WROTE]->(r1)-->()<--(r2)<-[:WROTE]-(p2)
WHERE id(p1) < id(p2) AND size((p2)-[:WROTE]->()) > 10
WITH p1,p2,count(*) as coop, collect(r1.stars) as s1, collect(r2.stars) as s2 where coop > 10
WITH p1,p2, apoc.algo.cosineSimilarity(s1,s2) as cosineSimilarity WHERE cosineSimilarity > 0
MERGE (p1)-[s:SIMILAR_REVIEWS]-(p2) SET s.weight = cosineSimilarity"
, {batchSize:100, parallel:false,iterateList:true});

// end::reviewsimilarity-graph[]


// tag::coocurence-graph[]

CALL apoc.periodic.iterate('
MATCH (b1:Business) 
WHERE size((b1)<-[:REVIEWS]->()) > 10 AND b1.city="Las Vegas" 
RETURN b1
','
MATCH (b1)<-[:REVIEWS]-(r1)
MATCH (r1)<-[:WROTE]-(u)
MATCH (u)-[:WROTE]->(r2)
MATCH (r2)-[:REVIEWS]->(b2)
WHERE id(b1) < id(b2) AND b2.city="Las Vegas"
WITH b1, b2, COUNT(*) AS weight where weight > 5
MERGE (b1)-[cr:CO_OCCURENT_REVIEWS]-(b2)
ON CREATE SET cr.weight = weight
',{batchSize: 1});

// end::coocurence-graph[]
