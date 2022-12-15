# GDS Docs README


## Setup

We use asciidoc for writing documentation, and we render it to both HTML and PDF.


### Rendering the docs locally

First, run `npm install` to install all dependencies.
You only need to do this once.
To build and view the docs locally, use `npm start`.
This will build the docs, start a web server hosting the docs at http://localhost:8000.

[NOTE]
----
If you experiene any kind of trouble, try `npm cache clear --force`
----


### A note on inline LaTeX: you can't

Currently, our toolchain cannot render LaTeX snippets into PDF (works for HTML tho!). So we are unable to use it.

What you can do though is use _cursive_, `monospace` and `.svg` images for anything more complicated.
https://www.codecogs.com/latex/eqneditor.php is helpful for inputting LaTeX and outputting `.svg` images, or any other image format for that matter.
We seem to use `.svg` so maybe stick to that.


### Documentation authoring

We use a few conventions for documentation source management:

1. Write one sentence per line.
   This keeps the source readable.
   A newline in the source has no effect on the produced content.
2. Use two empty lines ahead of a new section.
   This keeps the source more readable.


## Documentation testing

We do documentation testing where we scan documentation pages; extract setup queries, project queries and example queries; and execute them as tests.
A page is a natural boundary for a set of tests, and generally there is some data setup at the start, followed by several examples down the page.
This pattern aligns very well with telling a coherent story.

**NB:** You can have tests that span several pages. 


### Setup

Setup queries are executed first.
Think Cypher `CREATE` statements and their friends.
You need a database innit.
These happen once per page, the JUnit equivalent is `@BeforeAll`.
And that is an important point, we use JUnit as the driver for the tests, so we extract code snippets and turn pages into JUnit tests effectively.

```
[source, cypher, role=noplay setup-query]
----
CREATE (a:Location {name: 'A'}),
       (b:Location {name: 'B'}),
       (c:Location {name: 'C'}),
       (a)-[:ROAD {cost: 50}]->(b),
       (a)-[:ROAD {cost: 50}]->(c);
----
```

AsciiDoc parses the above _source block_ and applies some _cypher_ styling, those are both unrelated to testing.
So is the `role=noplay` attribute which controls rendering a button in the final documentation page.

What you need to know is that the `setup-query` puts the `CREATE` statement in among the `@BeforeAll` queries on a Neo4j instance that is lifecycled with the page.


### Project

Project queries happen next, taking data from Neo4j into GDS.
Scope here is, we give you a clean projection for each example, for safety.
This is implemented with JUnit `@BeforeEach` semantics.

```
[source, cypher, role=noplay graph-project-query]
----
CALL gds.graph.project(
    'myGraph',
    'Location',
    'ROAD',
    {
        relationshipProperties: 'cost'
    }
)
----
```

The interesting bit there is the `graph-project-query` attribute. 


### Demonstrate/ test

And lastly, we execute examples.
`@Test`s.
Here the convention is to have a Cypher statement with a procedure call first, and then a result table that gets turned into assertions about the output of the procedure call.

```
[role=query-example]
--
.The following will estimate the memory requirements for running the algorithm in write mode:
[source, cypher, role=noplay]
----
MATCH (source:Location {name: 'A'}), (target:Location {name: 'F'})
CALL gds.shortestPath.dijkstra.write.estimate('myGraph', {
    sourceNode: source,
    targetNode: target,
    relationshipWeightProperty: 'cost',
    writeRelationshipType: 'PATH'
})
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
RETURN nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
----

.Results
[opts="header"]
|===
| nodeCount | relationshipCount | bytesMin | bytesMax | requiredMemory
| 6         | 9                 | 696      | 696      | "696 Bytes"
|===
--
```

You can see the block structure where we start the outer block with

```
[role=query-example]
--
```

We then need to provide two blocks inside, a query and a result table. We demarcate the query with `----` and the result table is an Asciidoc table that starts with:

```
[opts="header"]
|===
```

Remember to match and close the blocks!


#### Ignoring results

Sometimes you want to run a test that has no assertions.
For this use case you have the `no-results` attribute:

```
It is now safe to drop the in-memory graph or shutdown the db, as we can restore it at a later point.
[role=query-example, no-result=true]
--
.The following will drop the in-memory graph:
[source, cypher, role=noplay]
----
CALL gds.graph.drop('myGraph')
----
--
```


#### Grouping tests

Sometimes you want your test to span multiple queries, so that one can observe side effects from another.
You use the `group` attribute.
Imagine you do first call a mutate procedure:

```
[role=query-example, group=louvain-conductance]
--
.The following will run the Louvain algorithm and store the results in `myGraph`:
[source, cypher, role=noplay]
----
CALL gds.louvain.mutate('myGraph', { mutateProperty: 'community', relationshipWeightProperty: 'weight' })
YIELD communityCount
----

.Results
[opts="header"]
|===
| communityCount
| 3
|===
--
```

Then further down the page you might want to illustrate streaming results: 

```
[role=query-example, group=louvain-conductance]
--
.The following will run the Conductance algorithm in `stream` mode:
[source, cypher, role=noplay]
----
CALL gds.alpha.conductance.stream('myGraph', { communityProperty: 'community', relationshipWeightProperty: 'weight' })
YIELD community, conductance
----

.Results
[opts="header"]
|===
| community | conductance
| 2         | 0.5
| 3         | 0.23076923076923078
| 5         | 0.2
|===
--
```

These two procedure calls are wrapped in a single test, and this within the scope of a single `@BeforeEach` (projection).


#### Impersonating a user

For some procedures the operator matters, and we have the `operator` attribute:

```
.Setting a default
[source, cypher, role=noplay setup-query, operator=Alicia]
----
CALL gds.alpha.config.defaults.set('concurrency', 12, 'Alicia')
----
```


#### Limitations

Remember, you are executing queries against an in-memory Neo4j Community database, so functional things like RBAC are not available.
And hence, you would not be able to illustrate any procedures requiring administrator-like privileges
