// tag::eda[]

CALL db.labels()
YIELD label
CALL apoc.cypher.run("MATCH (:`"+label+"`) RETURN count(*) as count", null)
YIELD value
RETURN label, value.count as count
ORDER BY label

// end::eda[]

// tag::eda-rels[]

CALL db.relationshipTypes()
YIELD relationshipType
CALL apoc.cypher.run("MATCH ()-[:" + `relationshipType` + "]->()
                      RETURN count(*) as count", null)
YIELD value
RETURN relationshipType, value.count AS count
ORDER BY relationshipType

// end::eda-rels[]

// tag::eda-hotels-intro[]

MATCH (category:Category {name: "Hotels"})
RETURN size((category)<-[:IN_CATEGORY]-()) AS businesses

// end::eda-hotels-intro[]


// tag::eda-hotels-cities[]

MATCH (category:Category {name: "Hotels"})<-[:IN_CATEGORY]-(business)-[:IN_CITY]->(city)
RETURN city.name AS city, count(*) as count
ORDER BY count DESC
LIMIT 10

// end::eda-hotels-cities[]

// tag::eda-hotels-reviews[]

MATCH (:Review)-[:REVIEWS]->(:Business)-[:IN_CATEGORY]->(:Category {name:'Hotels'})
RETURN count(*) AS count

// end::eda-hotels-reviews[]


// tag::eda-hotels-most-reviewed[]

MATCH (review:Review)-[:REVIEWS]->(business:Business),
      (business)-[:IN_CATEGORY]->(:Category {name:'Hotels'}),
      (business)-[:IN_CITY]->(:City {name: "Las Vegas"})
WITH business, count(*) AS reviews, avg(review.stars) AS averageRating
ORDER BY reviews DESC
LIMIT 10
RETURN business.name AS business,
       reviews,
       apoc.math.round(averageRating,2) AS averageRating

// end::eda-hotels-most-reviewed[]

// tag::hotel-reviewers-pagerank[]
CALL algo.pageRank(
    "MATCH (u:User)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->
           (:Category {name: 'Hotels'})
     WITH u, count(*) AS reviews
     WHERE reviews > 5
     RETURN id(u) AS id",
    "MATCH (u1:User)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->
           (:Category {name: 'Hotels'})
     MATCH (u1)-[:FRIENDS]->(u2)
     WHERE id(u1) < id(u2)
     RETURN id(u1) AS source, id(u2) AS target",
    {graph: "cypher", write: true, direction: "both", writeProperty: "hotelPageRank"}
)
// end::hotel-reviewers-pagerank[]

// tag::top-reviewers[]

MATCH (u:User)
WHERE u.hotelPageRank > 0
WITH u
ORDER BY u.hotelPageRank DESC
LIMIT 5
RETURN u.name AS name,
       apoc.math.round(u.hotelPageRank,2) AS pageRank,
       size((u)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->
            (:Category {name: "Hotels"})) AS hotelReviews,
       size((u)-[:WROTE]->()) AS totalReviews,
       size((u)-[:FRIENDS]-()) AS friends


// end::top-reviewers[]


// tag::caesars[]

MATCH (b:Business {name: "Caesars Palace Las Vegas Hotel & Casino"})
MATCH (b)<-[:REVIEWS]-(review)<-[:WROTE]-(user)
RETURN user.name AS name,
       apoc.math.round(user.hotelPageRank,2) AS pageRank,
       review.stars AS stars
ORDER BY user.hotelPageRank DESC
LIMIT 5


// end::caesars[]

// tag::lpa-super-category[]

CALL algo.labelPropagation.stream(
  "MATCH (c:Category) RETURN id(c) AS id",
  "MATCH (c1:Category)<-[:IN_CATEGORY]-()-[:IN_CATEGORY]->(c2:Category)
   WHERE id(c1) < id(c2)
   RETURN id(c1) AS source, id(c2) AS target, count(*) AS weight",
   {graph: "cypher"})
YIELD nodeId, label
MATCH (c:Category) WHERE id(c) = nodeId
MERGE (sc:SuperCategory {name: "SuperCategory-" + label})
MERGE (c)-[:IN_SUPER_CATEGORY]->(sc)

// end::lpa-super-category[]

// tag::lpa-hotels[]
MATCH (hotels:Category {name: "Hotels"}),
      (hotels)-[:IN_SUPER_CATEGORY]->()<-[:IN_SUPER_CATEGORY]-(otherCategory)
RETURN otherCategory.name AS otherCategory
LIMIT 5
// end::lpa-hotels[]


// tag::lpa-hotels-vegas[]

MATCH (hotels:Category {name: "Hotels"}),
      (lasVegas:City {name: "Las Vegas"}),
      (hotels)-[:IN_SUPER_CATEGORY]->()<-[:IN_SUPER_CATEGORY]-(otherCategory)
RETURN otherCategory.name AS otherCategory,
       size((otherCategory)<-[:IN_CATEGORY]-()-[:IN_CITY]->(lasVegas)) AS count
ORDER BY count DESC
LIMIT 10

// end::lpa-hotels-vegas[]

// tag::lpa-hotels-vegas-good-businesses[]

MATCH (hotels:Category {name: "Hotels"}),
      (lasVegas:City {name: "Las Vegas"}),
      (hotels)-[:IN_SUPER_CATEGORY]->()<-[:IN_SUPER_CATEGORY]-(otherCategory),
      (otherCategory)<-[:IN_CATEGORY]-(business)-[:IN_CITY]->(lasVegas)
WITH otherCategory, count(*) AS count,
     collect(business) AS businesses,
     apoc.coll.avg(collect(business.averageStars)) AS categoryAverageStars
ORDER BY count DESC
LIMIT 10
WITH otherCategory,
     [b in businesses where b.averageStars >= categoryAverageStars] AS businesses
RETURN otherCategory.name AS otherCategory,
       [b in businesses | b.name][toInteger(rand() * size(businesses))] AS business

// end::lpa-hotels-vegas-good-businesses[]
