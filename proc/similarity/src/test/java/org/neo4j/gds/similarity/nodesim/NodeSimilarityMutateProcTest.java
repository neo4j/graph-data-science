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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class NodeSimilarityMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:Person {id: 0,  name: 'Alice'})" +
        ", (b:Person {id: 1,  name: 'Bob'})" +
        ", (c:Person {id: 2,  name: 'Charlie'})" +
        ", (d:Person {id: 3,  name: 'Dave'})" +
        ", (i1:Item  {id: 10, name: 'p1'})" +
        ", (i2:Item  {id: 11, name: 'p2'})" +
        ", (i3:Item  {id: 12, name: 'p3'})" +
        ", (i4:Item  {id: 13, name: 'p4'})" +
        ", (a)-[:LIKES]->(i1)" +
        ", (a)-[:LIKES]->(i2)" +
        ", (a)-[:LIKES]->(i3)" +
        ", (b)-[:LIKES]->(i1)" +
        ", (b)-[:LIKES]->(i2)" +
        ", (c)-[:LIKES]->(i3)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            NodeSimilarityMutateProc.class,
            GraphProjectProc.class
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testMutateYields() {
        String graphProject = GdsCypher.call("myGraph")
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("LIKES")
            .yields();
        runQuery(graphProject);

        String query = GdsCypher.call("myGraph")
            .algo("nodeSimilarity")
            .mutateMode()
            .addParameter("similarityCutoff", 0.0)
            .addParameter("mutateRelationshipType", "FOO")
            .addParameter("mutateProperty", "foo")
            .yields(
                "computeMillis",
                "preProcessingMillis",
                "nodesCompared ",
                "relationshipsWritten",
                "mutateMillis",
                "similarityDistribution",
                "postProcessingMillis",
                "configuration"
            );

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("nodesCompared")).asInstanceOf(LONG).isEqualTo(3);
            assertThat(row.getNumber("relationshipsWritten")).asInstanceOf(LONG).isEqualTo(6);

            assertThat(row.getNumber("computeMillis"))
                .as("Missing computeMillis")
                .asInstanceOf(LONG)
                .isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("preProcessingMillis"))
                .as("Missing preProcessingMillis")
                .asInstanceOf(LONG)
                .isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("mutateMillis"))
                .as("Missing mutateMillis")
                .asInstanceOf(LONG)
                .isGreaterThanOrEqualTo(0);

            assertThat(row.get("similarityDistribution"))
                .asInstanceOf(MAP)
                .containsOnlyKeys("min", "max", "mean", "stdDev", "p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99", "p100")
                .allSatisfy((key, value) -> assertThat(value).asInstanceOf(DOUBLE).isGreaterThanOrEqualTo(0d));

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .as("Missing postProcessingMillis")
                .isEqualTo(-1L);

            assertThat(row.get("configuration"))
                .isNotNull()
                .isInstanceOf(Map.class);

        });

        assertThat(rowCount)
            .as("`mutate` mode should always return one row")
            .isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10})
    void shouldMutateUniqueRelationships(int topN) {
        var graphName = "undirectedGraph";

        var graphCreateQuery = GdsCypher.call(graphName)
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("LIKES", Orientation.UNDIRECTED)
            .yields();

        runQuery(graphCreateQuery);

        var query = GdsCypher.call(graphName)
            .algo("gds", "nodeSimilarity")
            .mutateMode()
            .addParameter("topK", 1)
            .addParameter("topN", topN)
            .addParameter("mutateRelationshipType", "SIMILAR")
            .addParameter("mutateProperty", "score")
            .yields("relationshipsWritten");

        var rowCount = runQueryWithRowConsumer(
            query,
            row -> assertThat(row.getNumber("relationshipsWritten")).asInstanceOf(LONG).isEqualTo(6)
        );

        assertThat(rowCount)
            .as("`mutate` mode should always return one row")
            .isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10})
    void shouldMutateWithFilteredNodes(int topN) {
        runQuery("MATCH (n) DETACH DELETE n");
        String graphCreateQuery =
            "CREATE (alice:Person)" +
            ", (carol:Person)" +
            ", (eve:Person)" +
            ", (dave:Foo)" +
            ", (bob:Foo)" +
            ", (a:Bar)" +
            ", (dave)-[:KNOWS]->(a)" +
            ", (bob)-[:KNOWS]->(a)";
        runQuery(graphCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeLabel("Foo")
            .withNodeLabel("Bar")
            .withRelationshipType("KNOWS")
            .yields();
        runQuery(createQuery);

        String relationshipType = "SIMILAR";
        String relationshipProperty = "score";

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.nodeSimilarity")
            .mutateMode()
            .addParameter("nodeLabels", List.of("Foo", "Bar"))
            .addParameter("mutateRelationshipType", relationshipType)
            .addParameter("mutateProperty", relationshipProperty)
            .addParameter("topN", topN)
            .yields();
        runQuery(algoQuery);

        Graph mutatedGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph").graphStore().getUnion();

        assertGraphEquals(
            fromGdl(
                graphCreateQuery +
                ", (dave)-[:SIMILAR {score: 1.0}]->(bob)" +
                ", (bob)-[:SIMILAR {score: 1.0}]->(dave)"
            ),
            mutatedGraph
        );
    }
}
