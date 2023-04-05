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
package org.neo4j.gds.similarity.filteredknn;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class FilteredKnnMutateProcTest extends BaseProcTest {


    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a { id: 1, knn: 1.0 } )" +
        ", (b { id: 2, knn: 2.0 } )" +
        ", (c { id: 3, knn: 5.0 } )" +
        ", (a)-[:IGNORE]->(b)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            FilteredKnnMutateProc.class,
            GraphProjectProc.class
        );

        String graphCreateQuery = GdsCypher.call("filteredKnnGraph")
            .graphProject()
            .withAnyLabel()
            .withNodeProperty("knn")
            .withRelationshipType("IGNORE")
            .yields();

        runQuery(graphCreateQuery);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldMutateResults() {
        String query = GdsCypher.call("filteredKnnGraph")
            .algo("gds.alpha.knn.filtered")
            .mutateMode()
            .addParameter("sudo", true)
            .addParameter("nodeProperties", List.of("knn"))
            .addParameter("topK", 1)
            .addParameter("randomSeed", 42)
            .addParameter("concurrency", 1)
            .addParameter("mutateRelationshipType", "SIMILAR")
            .addParameter("mutateProperty", "score")
            .yields();

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("nodesCompared")).asInstanceOf(LONG).isEqualTo(3);
            assertThat(row.getNumber("relationshipsWritten")).asInstanceOf(LONG).isEqualTo(3);
            assertThat(row.getNumber("nodePairsConsidered")).asInstanceOf(LONG).isEqualTo(37);
            assertThat(row.getBoolean("didConverge")).isTrue();
            assertThat(row.getNumber("ranIterations")).asInstanceOf(LONG).isEqualTo(1);

            assertUserInput(row, "mutateRelationshipType", "SIMILAR");
            assertUserInput(row, "mutateProperty", "score");

            assertThat(row.getNumber("computeMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("preProcessingMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("mutateMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("postProcessingMillis")).asInstanceOf(LONG).isEqualTo(-1);

            assertThat(row.get("similarityDistribution"))
                .asInstanceOf(MAP)
                .containsOnlyKeys("min", "max", "mean", "stdDev", "p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99", "p100")
                .allSatisfy((key, value) -> assertThat(value).asInstanceOf(DOUBLE).isGreaterThanOrEqualTo(0d));
        });

        assertThat(rowCount)
            .as("`mutate` mode should always return one row")
            .isEqualTo(1);
    }

    @Test
    void shouldMutateUniqueRelationships() {
        var graphName = "undirectedGraph";

        var graphCreateQuery = GdsCypher.call(graphName)
            .graphProject()
            .withAnyLabel()
            .withNodeProperty("knn")
            .withRelationshipType("IGNORE", Orientation.UNDIRECTED)
            .yields();

        runQuery(graphCreateQuery);

        var query = GdsCypher.call(graphName)
            .algo("gds.alpha.knn.filtered")
            .mutateMode()
            .addParameter("sudo", true)
            .addParameter("nodeProperties", List.of("knn"))
            .addParameter("topK", 1)
            .addParameter("concurrency", 1)
            .addParameter("randomSeed", 42)
            .addParameter("mutateRelationshipType", "SIMILAR")
            .addParameter("mutateProperty", "score")
            .yields("relationshipsWritten");

        var rowCount = runQueryWithRowConsumer(
            query,
            row -> assertThat(row.getNumber("relationshipsWritten"))
                .asInstanceOf(LONG)
                .isEqualTo(3)
        );

        assertThat(rowCount)
            .as("`mutate` mode should always return one row")
            .isEqualTo(1);
    }

    @Test
    void shouldMutateWithFilteredNodes() {
        String nodeCreateQuery =
            "CREATE " +
            "  (alice:Person {age: 24})" +
            " ,(carol:Person {age: 24})" +
            " ,(eve:Person {age: 67})" +
            " ,(dave:Foo {age: 48})" +
            " ,(bob:Foo {age: 48})";

        runQuery(nodeCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeLabel("Foo")
            .withNodeProperty("age")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String relationshipType = "SIMILAR";
        String relationshipProperty = "score";

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.knn.filtered")
            .mutateMode()
            .addParameter("nodeLabels", List.of("Foo"))
            .addParameter("nodeProperties", List.of("age"))
            .addParameter("mutateRelationshipType", relationshipType)
            .addParameter("mutateProperty", relationshipProperty).yields();
        runQuery(algoQuery);

        Graph mutatedGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph").graphStore().getUnion();

        assertGraphEquals(
            fromGdl(
                nodeCreateQuery +
                "(dave)-[:SIMILAR {score: 1.0}]->(bob)" +
                "(bob)-[{score: 1.0}]->(dave)"
            ),
            mutatedGraph
        );
    }

    @Test
    void shouldMutateWithSimilarityThreshold() {
        String relationshipType = "SIMILAR";
        String relationshipProperty = "score";

        String nodeCreateQuery =
            "CREATE " +
            "  (alice:Person {age: 23})" +
            " ,(carol:Person {age: 24})" +
            " ,(eve:Person {age: 34})" +
            " ,(bob:Person {age: 30})";

        runQuery(nodeCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeProperty("age")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.knn.filtered")
            .mutateMode()
            .addParameter("nodeProperties", List.of("age"))
            .addParameter("similarityCutoff", 0.14)
            .addParameter("mutateRelationshipType", relationshipType)
            .addParameter("mutateProperty", relationshipProperty).yields();
        runQuery(algoQuery);

        Graph mutatedGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph").graphStore().getUnion();

        assertThat(mutatedGraph.relationshipCount()).isEqualTo(6);

    }

    @Test
    void shouldMutateWithFilteredNodesAndMultipleProperties() {
        String nodeCreateQuery =
            "CREATE " +
            "  (alice:Person {age: 24, knn: 24})" +
            " ,(carol:Person {age: 24, knn: 24})" +
            " ,(eve:Person {age: 67, knn: 67})" +
            " ,(dave:Foo {age: 48, knn: 24})" +
            " ,(bob:Foo {age: 48, knn: 48} )";

        runQuery(nodeCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeLabel("Foo")
            .withNodeProperty("age")
            .withNodeProperty("knn")

            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.knn.filtered")
            .mutateMode()
            .addParameter("nodeLabels", List.of("Foo"))
            .addParameter("nodeProperties", List.of("age", "knn"))
            .addParameter("mutateRelationshipType", "SIMILAR")
            .addParameter("mutateProperty", "score")
            .yields();

        runQuery(algoQuery);

        Graph mutatedGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph").graphStore().getUnion();

        assertGraphEquals(
            fromGdl(
                nodeCreateQuery +
                //  0.5 * (1/(1+(48-48)) + 0.5 *(1/(1+(48-24)) = 0.52
                "(dave)-[:SIMILAR {score: 0.52}]->(bob)" +
                "(bob)-[{score: 0.52}]->(dave)"
            ),
            mutatedGraph
        );
    }

}
