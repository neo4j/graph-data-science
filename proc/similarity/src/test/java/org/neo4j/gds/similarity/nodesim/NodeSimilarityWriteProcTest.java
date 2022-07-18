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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.WriteRelationshipWithPropertyTest;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.Orientation.REVERSE;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodeSimilarityWriteProcTest
    extends NodeSimilarityProcTest<NodeSimilarityWriteConfig>
    implements WriteRelationshipWithPropertyTest<NodeSimilarity, NodeSimilarityWriteConfig, NodeSimilarityResult> {

    @Override
    public Class<? extends AlgoBaseProc<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig, ?>> getProcedureClazz() {
        return NodeSimilarityWriteProc.class;
    }

    @Override
    public NodeSimilarityWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return NodeSimilarityWriteConfig.of(mapWrapper);
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("org.neo4j.gds.similarity.nodesim.NodeSimilarityProcTest#allValidGraphVariationsWithProjections")
    void shouldWriteResults(GdsCypher.QueryBuilder queryBuilder, Orientation orientation, String testName) {
        String query = queryBuilder
            .algo("nodeSimilarity")
            .writeMode()
            .addParameter("similarityCutoff", 0.0)
            .addParameter("writeRelationshipType", "SIMILAR")
            .addParameter("writeProperty", "score")
            .yields(
                "computeMillis",
                "preProcessingMillis",
                "nodesCompared ",
                "relationshipsWritten",
                "writeMillis",
                "similarityDistribution",
                "postProcessingMillis",
                "configuration"
            );

        runQueryWithRowConsumer(query, row -> {
            assertEquals(3, row.getNumber("nodesCompared").longValue());
            assertEquals(6, row.getNumber("relationshipsWritten").longValue());
            assertUserInput(row, "writeRelationshipType", "SIMILAR");
            assertUserInput(row, "writeProperty", "score");
            assertThat("Missing computeMillis", -1L, lessThan(row.getNumber("computeMillis").longValue()));
            assertThat("Missing preProcessingMillis", -1L, lessThan(row.getNumber("preProcessingMillis").longValue()));
            assertThat("Missing writeMillis", -1L, lessThan(row.getNumber("writeMillis").longValue()));

            Map<String, Double> distribution = (Map<String, Double>) row.get("similarityDistribution");
            assertThat("Missing min", -1.0, lessThan(distribution.get("min")));
            assertThat("Missing max", -1.0, lessThan(distribution.get("max")));
            assertThat("Missing mean", -1.0, lessThan(distribution.get("mean")));
            assertThat("Missing stdDev", -1.0, lessThan(distribution.get("stdDev")));
            assertThat("Missing p1", -1.0, lessThan(distribution.get("p1")));
            assertThat("Missing p5", -1.0, lessThan(distribution.get("p5")));
            assertThat("Missing p10", -1.0, lessThan(distribution.get("p10")));
            assertThat("Missing p25", -1.0, lessThan(distribution.get("p25")));
            assertThat("Missing p50", -1.0, lessThan(distribution.get("p50")));
            assertThat("Missing p75", -1.0, lessThan(distribution.get("p75")));
            assertThat("Missing p90", -1.0, lessThan(distribution.get("p90")));
            assertThat("Missing p95", -1.0, lessThan(distribution.get("p95")));
            assertThat("Missing p99", -1.0, lessThan(distribution.get("p99")));
            assertThat("Missing p100", -1.0, lessThan(distribution.get("p100")));

            assertThat(
                "Missing postProcessingMillis",
                -1L,
                equalTo(row.getNumber("postProcessingMillis").longValue())
            );
        });

        String resultGraphName = "simGraph_" + orientation.name();
        String loadQuery = GdsCypher.call(resultGraphName)
            .graphProject()
            .withNodeLabel(orientation == REVERSE ? "Item" : "Person")
            .withRelationshipType("SIMILAR", orientation)
            .withNodeProperty("id")
            .withRelationshipProperty("score")
            .yields();

        runQuery(loadQuery);

        assertGraphEquals(
            orientation == REVERSE
                ? fromGdl(
                formatWithLocale(
                    "  (i1:Item {id: 10})" +
                    ", (i2:Item {id: 11})" +
                    ", (i3:Item {id: 12})" +
                    ", (i4:Item {id: 13})" +
                    ", (i1)-[:SIMILAR {w: %f}]->(i2)" +
                    ", (i1)-[:SIMILAR {w: %f}]->(i3)" +
                    ", (i2)-[:SIMILAR {w: %f}]->(i1)" +
                    ", (i2)-[:SIMILAR {w: %f}]->(i3)" +
                    ", (i3)-[:SIMILAR {w: %f}]->(i1)" +
                    ", (i3)-[:SIMILAR {w: %f}]->(i2)",
                    1 / 1.0,
                    1 / 3.0,
                    1 / 1.0,
                    1 / 3.0,
                    1 / 3.0,
                    1 / 3.0
                )
            )
                : fromGdl(
                    formatWithLocale(
                        "  (a:Person {id: 0})" +
                        ", (b:Person {id: 1})" +
                        ", (c:Person {id: 2})" +
                        ", (d:Person {id: 3})" +
                        ", (a)-[:SIMILAR {w: %f}]->(b)" +
                        ", (a)-[:SIMILAR {w: %f}]->(c)" +
                        ", (b)-[:SIMILAR {w: %f}]->(c)" +
                        ", (b)-[:SIMILAR {w: %f}]->(a)" +
                        ", (c)-[:SIMILAR {w: %f}]->(a)" +
                        ", (c)-[:SIMILAR {w: %f}]->(b)",
                        2 / 3.0,
                        1 / 3.0,
                        0.0,
                        2 / 3.0,
                        1 / 3.0,
                        0.0
                    )
                ),
            GraphStoreCatalog.get(getUsername(), namedDatabaseId(), resultGraphName).graphStore().getUnion()
        );
    }

    @ParameterizedTest(name = "missing parameter: {0}")
    @ValueSource(strings = {"writeProperty", "writeRelationshipType"})
    void shouldFailIfConfigIsMissingWriteParameters(String parameter) {
        CypherMapWrapper input = createMinimalConfig(CypherMapWrapper.empty())
            .withoutEntry(parameter);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> createConfig(input)
        );
        assertThat(
            illegalArgumentException.getMessage(),
            startsWith(formatWithLocale("No value specified for the mandatory configuration parameter `%s`", parameter))
        );
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            mapWrapper = mapWrapper.withString("writeProperty", writeProperty());
        }
        if (!mapWrapper.containsKey("writeRelationshipType")) {
            mapWrapper = mapWrapper.withString("writeRelationshipType", writeRelationshipType());
        }
        return mapWrapper;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10})
    void shouldWriteUniqueRelationships(int topN) {
        var graphName = "undirectedGraph";

        var graphCreateQuery = GdsCypher.call(graphName)
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("LIKES", Orientation.UNDIRECTED)
            .yields();

        runQuery(graphCreateQuery);

        var query = GdsCypher.call(graphName)
            .algo("gds", "nodeSimilarity")
            .writeMode()
            .addParameter("sudo", true)
            .addParameter("topK", 1)
            .addParameter("topN", topN)
            .addParameter("writeRelationshipType", "SIMILAR")
            .addParameter("writeProperty", "score")
            .yields("relationshipsWritten");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(6, row.getNumber("relationshipsWritten").longValue());
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10})
    void shouldWriteWithFilteredNodes(int topN) {
        runQuery("MATCH (n) DETACH DELETE n");
        runQuery("CREATE (alice:Person {name: 'Alice'})" +
                 "CREATE (carol:Person {name: 'Carol'})" +
                 "CREATE (eve:Person {name: 'Eve'})" +
                 "CREATE (dave:Foo {name: 'Dave'})" +
                 "CREATE (bob:Foo {name: 'Bob'})" +
                 "CREATE (a:Bar)" +
                 "CREATE (dave)-[:KNOWS]->(a)" +
                 "CREATE (bob)-[:KNOWS]->(a)");

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeLabel("Foo")
            .withNodeLabel("Bar")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String relationshipType = "SIMILAR";
        String relationshipProperty = "score";

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.nodeSimilarity")
            .writeMode()
            .addParameter("nodeLabels", List.of("Foo", "Bar"))
            .addParameter("writeRelationshipType", relationshipType)
            .addParameter("topN", topN)
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

    @Override
    public String writeRelationshipType() {
        return "NODE_SIM_REL";
    }

    @Override
    public String writeProperty() {
        return "similarity";
    }
}
