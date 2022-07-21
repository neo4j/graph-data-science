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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateRelationshipWithPropertyTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class NodeSimilarityMutateProcTest
    extends NodeSimilarityProcTest<NodeSimilarityMutateConfig>
    implements MutateRelationshipWithPropertyTest<NodeSimilarity, NodeSimilarityMutateConfig, NodeSimilarityResult> {

    @Override
    public String mutateRelationshipType() {
        return "SIMILAR_TO";
    }

    @Override
    public String mutateProperty() {
        return "similarity";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.DOUBLE;
    }

    @Override
    public Class<? extends AlgoBaseProc<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig, ?>> getProcedureClazz() {
        return NodeSimilarityMutateProc.class;
    }

    @Override
    public NodeSimilarityMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return NodeSimilarityMutateConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("mutateProperty")) {
            mapWrapper = mapWrapper.withString("mutateProperty", mutateProperty());
        }
        if (!mapWrapper.containsKey("mutateRelationshipType")) {
            mapWrapper = mapWrapper.withString("mutateRelationshipType", mutateRelationshipType());
        }
        return mapWrapper;
    }

    @Override
    public String expectedMutatedGraph() {
        return formatWithLocale(
            "  (a)" +
            ", (b)" +
            ", (c)" +
            ", (d)" +
            ", (i1)" +
            ", (i2)" +
            ", (i3)" +
            ", (i4)" +
            // LIKES
            ", (a)-[{w: 1.0d}]->(i1)" +
            ", (a)-[{w: 1.0d}]->(i2)" +
            ", (a)-[{w: 1.0d}]->(i3)" +
            ", (b)-[{w: 1.0d}]->(i1)" +
            ", (b)-[{w: 1.0d}]->(i2)" +
            ", (c)-[{w: 1.0d}]->(i3)" +
            // SIMILAR_TO
            ", (a)-[:SIMILAR_TO {w: %f}]->(b)" +
            ", (a)-[:SIMILAR_TO {w: %f}]->(c)" +
            ", (b)-[:SIMILAR_TO {w: %f}]->(a)" +
            ", (c)-[:SIMILAR_TO {w: %f}]->(a)",
            2 / 3.0,
            1 / 3.0,
            2 / 3.0,
            1 / 3.0
        );
    }

    @Test
    void testMutateYields() {
        loadGraph("graph");

        String query = GdsCypher.call("graph")
            .algo("nodeSimilarity")
            .mutateMode()
            .addParameter("similarityCutoff", 0.0)
            .addParameter("mutateRelationshipType", mutateRelationshipType())
            .addParameter("mutateProperty", mutateProperty())
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

        runQueryWithRowConsumer(query, row -> {
            assertEquals(3, row.getNumber("nodesCompared").longValue());
            assertEquals(6, row.getNumber("relationshipsWritten").longValue());
            assertThat("Missing computeMillis", -1L, lessThan(row.getNumber("computeMillis").longValue()));
            assertThat("Missing preProcessingMillis", -1L, lessThan(row.getNumber("preProcessingMillis").longValue()));
            assertThat("Missing mutateMillis", -1L, lessThan(row.getNumber("mutateMillis").longValue()));

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
    }

    @Override
    @Test
    @Disabled("This test does not work for NodeSimilarity")
    public void testGraphMutationOnFilteredGraph() { }

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
            .addParameter("sudo", true)
            .addParameter("topK", 1)
            .addParameter("topN", topN)
            .addParameter("mutateRelationshipType", "SIMILAR")
            .addParameter("mutateProperty", "score")
            .yields("relationshipsWritten");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(6, row.getNumber("relationshipsWritten").longValue());
        });
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
