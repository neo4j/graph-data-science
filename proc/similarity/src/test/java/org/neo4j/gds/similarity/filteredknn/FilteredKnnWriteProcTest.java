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
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class FilteredKnnWriteProcTest extends BaseProcTest {

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
            FilteredKnnWriteProc.class,
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
    void shouldWriteResults() {
        String query = GdsCypher.call("filteredKnnGraph")
            .algo("gds.alpha.knn.filtered")
            .writeMode()
            .addParameter("sudo", true)
            .addParameter("nodeProperties", List.of("knn"))
            .addParameter("topK", 1)
            .addParameter("concurrency", 1)
            .addParameter("randomSeed", 42)
            .addParameter("writeRelationshipType", "SIMILAR")
            .addParameter("writeProperty", "score")
            .yields();

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("nodesCompared")).asInstanceOf(LONG).isEqualTo(3);
            assertThat(row.getNumber("relationshipsWritten")).asInstanceOf(LONG).isEqualTo(3);
            assertThat(row.getNumber("nodePairsConsidered")).asInstanceOf(LONG).isEqualTo(37);
            assertThat(row.getBoolean("didConverge")).isTrue();
            assertThat(row.getNumber("ranIterations")).asInstanceOf(LONG).isEqualTo(1);

            assertUserInput(row, "writeRelationshipType", "SIMILAR");
            assertUserInput(row, "writeProperty", "score");

            assertThat(row.getNumber("computeMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("preProcessingMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("writeMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("postProcessingMillis")).asInstanceOf(LONG).isEqualTo(-1);

            assertThat(row.get("similarityDistribution"))
                .asInstanceOf(MAP)
                .containsOnlyKeys("min", "max", "mean", "stdDev", "p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99", "p100")
                .allSatisfy((key, value) -> assertThat(value).asInstanceOf(DOUBLE).isGreaterThanOrEqualTo(0d));

        });

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);

        String resultGraphName = "simGraph";
        String loadQuery = GdsCypher.call(resultGraphName)
            .graphProject()
            .withAnyLabel()
            .withNodeProperty("id")
            .withRelationshipType("SIMILAR")
            .withRelationshipProperty("score")
            .yields();

        runQuery(loadQuery);

        assertGraphEquals(
            fromGdl(
                "(a {id: 1})-[:SIMILAR {w: 0.5}]->(b {id: 2}), (b)-[:SIMILAR {w: 0.5}]->(a), (c {id: 3})-[:SIMILAR {w: 0.25}]->(b)"),
            GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), resultGraphName).graphStore().getUnion()
        );
    }

    @Test
    void shouldWriteUniqueRelationships() {
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
            .writeMode()
            .addParameter("sudo", true)
            .addParameter("nodeProperties", List.of("knn"))
            .addParameter("topK", 1)
            .addParameter("concurrency", 1)
            .addParameter("randomSeed", 42)
            .addParameter("writeRelationshipType", "SIMILAR")
            .addParameter("writeProperty", "score")
            .yields("relationshipsWritten");

        var rowCount = runQueryWithRowConsumer(query, row -> assertEquals(3, row.getNumber("relationshipsWritten").longValue()));

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);
    }

    @Test
    void shouldWriteWithFilteredNodes() {
        runQuery("CREATE (alice:Person {name: 'Alice', age: 24})" +
                 "CREATE (carol:Person {name: 'Carol', age: 24})" +
                 "CREATE (eve:Person {name: 'Eve', age: 67})" +
                 "CREATE (dave:Foo {name: 'Dave', age: 48})" +
                 "CREATE (bob:Foo {name: 'Bob', age: 48})");

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
            .writeMode()
            .addParameter("nodeLabels", List.of("Foo"))
            .addParameter("nodeProperties", List.of("age"))
            .addParameter("writeRelationshipType", relationshipType)
            .addParameter("writeProperty", relationshipProperty).yields();
        runQuery(algoQuery);

        Graph knnGraph = new StoreLoaderBuilder()
            .databaseService(db)
            .addNodeLabel("Person")
            .addNodeLabel("Foo")
            .addRelationshipType(relationshipType)
            .addRelationshipProperty(relationshipProperty, relationshipProperty, DefaultValue.DEFAULT, Aggregation.NONE)
            .build()
            .graph();

        assertGraphEquals(
            fromGdl("(alice:Person)" +
                    "(carol:Person)" +
                    "(eve:Person)" +
                    "(dave:Foo)" +
                    "(bob:Foo)" +
                    "(dave)-[:SIMILAR {score: 1.0}]->(bob)" +
                    "(bob)-[:SIMILAR {score: 1.0}]->(dave)"
            ),
            knnGraph
        );
    }
}
