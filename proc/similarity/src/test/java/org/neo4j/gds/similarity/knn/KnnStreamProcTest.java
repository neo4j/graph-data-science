/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.similarity.knn;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class KnnStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
        "  (a { id: 1, knn: 1.0 } )" +
        ", (b { id: 2, knn: 2.0 } )" +
        ", (c { id: 3, knn: 5.0 } )" +
        ", (a)-[:IGNORE]->(b)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            KnnStreamProc.class,
            GraphProjectProc.class
        );
    }

    @Test
    void shouldStreamResults() {
        runQuery("CALL gds.graph.project('myGraph', {__ALL__: {label: '*', properties: 'knn'}}, 'IGNORE')");

        var query = "CALL gds.knn.stream($graph, {nodeProperties: ['knn'], topK: 1, randomSeed: 19, concurrency: 1})" +
            " YIELD node1, node2, similarity" +
            " RETURN node1, node2, similarity" +
            " ORDER BY node1";
        assertCypherResult(
            query,
            Map.of("graph", "myGraph"),
            List.of(
                Map.of("node1", idFunction.of("a"), "node2", idFunction.of("b"), "similarity", 0.5),
                Map.of("node1", idFunction.of("b"), "node2", idFunction.of("a"), "similarity", 0.5),
                Map.of("node1", idFunction.of("c"), "node2", idFunction.of("b"), "similarity", 0.25)
            )
        );
    }

    @Test
    void shouldStreamWithFilteredNodes() {
        clearDb();

        String nodeCreateQuery = "CREATE " +
            "  (alice:Person {name: 'alice', age: 24})" +
            " ,(carol:Person {name: 'carol', age: 24})" +
            " ,(eve:Person {name: 'eve', age: 67})" +
            " ,(dave:Foo {name: 'dave', age: 48})" +
            " ,(bob:Foo {name: 'bob', age: 48})";

        runQuery(nodeCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeLabel("Foo")
            .withNodeProperty("age")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        var idMap = runQuery(
            "Match (n:Foo) RETURN id(n) AS id, n.name AS name",
            result -> result.stream().collect(Collectors.toMap(o -> ((String) o.get("name")), o -> ((Long) o.get("id"))))
        );

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.knn")
            .streamMode()
            .addParameter("nodeLabels", List.of("Foo"))
            .addParameter("nodeProperties", List.of("age"))
            .yields("node1", "node2", "similarity");
        assertCypherResult(
            algoQuery,
            List.of(
                Map.of("node1", idMap.get("dave"), "node2", idMap.get("bob"), "similarity", 1.0),
                Map.of("node1", idMap.get("bob"), "node2", idMap.get("dave"), "similarity", 1.0)
            )
        );
    }

    @Test
    void computeOverSparseNodeProperties() {
        String nodeCreateQuery = "CREATE " +
            "  (alice:Person {grades: [24, 4]})" +
            " ,(eve:Person)" +
            " ,(bob:Foo {grades: [24, 4, 42]})";

        runQuery(nodeCreateQuery);

        String createQuery = "CALL gds.graph.project('graph', " +
            "'Person', '*', {nodeProperties: {grades: {defaultValue: [1, 1]}}})";
        runQuery(createQuery);

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.knn")
            .streamMode()
            .addParameter("nodeLabels", List.of("Person"))
            .addParameter("nodeProperties", List.of("grades"))
            .yields("node1", "node2", "similarity");

        runQueryWithResultConsumer(algoQuery, result -> assertThat(result.stream().count()).isEqualTo(2));
    }
}
