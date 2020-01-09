// tag::create-sample-graph[]

MERGE (nAlice:User {id:'Alice'})
MERGE (nBridget:User {id:'Bridget'})
MERGE (nCharles:User {id:'Charles'})
MERGE (nDoug:User {id:'Doug'})
MERGE (nMark:User {id:'Mark'})
MERGE (nMichael:User {id:'Michael'})

MERGE (nAlice)-[:FOLLOWS]->(nDoug)
MERGE (nAlice)-[:FOLLOWS]->(nBridget)
MERGE (nAlice)-[:FOLLOWS]->(nCharles)
MERGE (nMark)-[:FOLLOWS]->(nDoug)
MERGE (nMark)-[:FOLLOWS]->(nMichael)
MERGE (nBridget)-[:FOLLOWS]->(nDoug)
MERGE (nCharles)-[:FOLLOWS]->(nDoug)
MERGE (nMichael)-[:FOLLOWS]->(nDoug)

// end::create-sample-graph[]

// tag::create-sample-weighted-graph[]

MERGE (nAlice:User {id:'Alice'})
MERGE (nBridget:User {id:'Bridget'})
MERGE (nCharles:User {id:'Charles'})
MERGE (nDoug:User {id:'Doug'})
MERGE (nMark:User {id:'Mark'})
MERGE (nMichael:User {id:'Michael'})

MERGE (nAlice)-[:FOLLOWS {score: 1}]->(nDoug)
MERGE (nAlice)-[:FOLLOWS {score: 2}]->(nBridget)
MERGE (nAlice)-[:FOLLOWS {score: 5}]->(nCharles)
MERGE (nMark)-[:FOLLOWS {score: 1.5}]->(nDoug)
MERGE (nMark)-[:FOLLOWS {score: 4.5}]->(nMichael)
MERGE (nBridget)-[:FOLLOWS {score: 1.5}]->(nDoug)
MERGE (nCharles)-[:FOLLOWS {score: 2}]->(nDoug)
MERGE (nMichael)-[:FOLLOWS {score: 1.5}]->(nDoug)

// end::create-sample-weighted-graph[]

// tag::stream-sample-graph-followers[]
CALL gds.alpha.degree.stream({
  nodeProjection: 'User',
  relationshipProjection: {
    FOLLOWS: {
      type: 'FOLLOWS',
      projection: 'reverse'
    }
  }
})
YIELD nodeId, score
RETURN algo.asNode(nodeId).id AS name, score AS followers
ORDER BY followers DESC
// end::stream-sample-graph-followers[]

// tag::write-sample-graph-followers[]
CALL gds.alpha.degree.write({
  nodeProperty: 'User',
  relationshipProperty: {
    FOLLOWS: {
      type: 'FOLLOWS',
      projection: 'reverse'
    }
  },
  writeProperty: 'followers'
})
// end::write-sample-graph-followers[]

// tag::stream-sample-graph-following[]
CALL gds.alpha.degree.stream({
  nodeProjection: 'User',
  relationshipProjection: 'FOLLOWS'
})
YIELD nodeId, score
RETURN algo.asNode(nodeId).id AS name, score AS followers
ORDER BY followers DESC
// end::stream-sample-graph-following[]

// tag::write-sample-graph-following[]
CALL gds.alpha.degree.write({
  nodeProperty: 'User',
  relationshipProperty: 'FOLLOWS',
  writeProperty: 'followers'
})
// end::write-sample-graph-following[]



// tag::stream-sample-weighted-graph-followers[]
CALL gds.alpha.degree.stream({
   nodeProjection: 'User',
   relationshipProjection: {
       FOLLOWS: {
           type: 'FOLLOWS',
           projection: 'reverse',
           properties: {
               score: {
                   property: 'score'
               }
           }
       }
   },
   weightProperty: 'score'
})
YIELD nodeId, score
RETURN algo.asNode(nodeId).id AS name, score AS weightedFollowers
// end::stream-sample-weighted-graph-followers[]

// tag::write-sample-weighted-graph-followers[]
CALL gds.alpha.degree.write({
   nodeProjection: 'User', 
   relationshipProjection: {
       FOLLOWS: {
           type: 'FOLLOWS',
           projection: 'reverse',
           properties: {
               score: {
                   property: 'score'
               }
           }
       }
   },
   weightProperty: 'score',
   writeProperty: 'weightedFollowers'
})
YIELD nodes, writeProperty
// end::write-sample-weighted-graph-followers[]

// tag::huge-projection[]

CALL gds.alpha.degree.stream({
  nodeProjections: 'User',
  relationshipProjection: 'FOLLOWS'
});

// end::huge-projection[]

// tag::cypher-loading[]

CALL gds.alpha.degree.write({
  nodeQuery: 'MATCH (u:User) RETURN id(u) as id',
  relationshipQuery: 'MATCH (u1:User)<-[:FOLLOWS]-(u2:User) RETURN id(u1) as source, id(u2) as target',
  writeProperty: 'followers'
})

// end::cypher-loading[]

// tag::cypher-loading-following[]

CALL gds.alpha.degree.write({
  nodeQuery: 'MATCH (u:User) RETURN id(u) as id',
  relationshipQuery: 'MATCH (u1:User)-[:FOLLOWS]->(u2:User) RETURN id(u1) as source, id(u2) as target',
  writeProperty: 'followers'
})

// end::cypher-loading-following[]