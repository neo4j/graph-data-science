// tag::unweighted-stream-sample-graph[]


// end::unweighted-stream-sample-graph[]

// tag::unweighted-write-sample-graph[]

// end::unweighted-write-sample-graph[]

// tag::weighted-stream-sample-graph[]


// end::weighted-stream-sample-graph[]

// tag::weighted-write-sample-graph[]

CALL algo.unionFind('User', 'FRIEND', {write:true, partitionProperty:"partition",weightProperty:'weight', defaultValue:0.0, threshold:1.0, concurrency: 1})
YIELD nodes, setCount, loadMillis, computeMillis, writeMillis;

// end::weighted-write-sample-graph[]

// tag::count-component-yelp[]

CALL algo.unionFind.stream('User', 'FRIEND', {}) 
YIELD nodeId,setId
RETURN count(distinct setId) as count_of_components;

// end::count-component-yelp[]

// tag::top-20-component-yelp[]

CALL algo.unionFind.stream('User', 'FRIEND', {}) 
YIELD nodeId,setId
RETURN setId,count(*) as size_of_component
ORDER BY size_of_component
LIMIT 20;

// end::top-20-component-yelp[]

// tag::cypher-loading[]

CALL algo.unionFind(
  'MATCH (p:User) RETURN id(p) as id',
  'MATCH (p1:User)-[f:FRIEND]->(p2:User) 
   RETURN id(p1) as source, id(p2) as target, f.weight as weight',
  {graph:'cypher',write:true}
);

// end::cypher-loading[]


// tag::huge-projection[]

CALL algo.unionFind('User', 'FRIEND', {graph:'huge'}) 
YIELD nodes, setCount, loadMillis, computeMillis, writeMillis;

// end::huge-projection[]
