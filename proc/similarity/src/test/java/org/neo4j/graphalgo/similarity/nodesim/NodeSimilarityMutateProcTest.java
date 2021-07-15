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
package org.neo4j.graphalgo.similarity.nodesim;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.MutateRelationshipWithPropertyTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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
    public Class<? extends AlgoBaseProc<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig>> getProcedureClazz() {
        return NodeSimilarityMutateProc.class;
    }

    @Override
    public NodeSimilarityMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return NodeSimilarityMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
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
            ", (a)-[{w: %f}]->(b)" +
            ", (a)-[{w: %f}]->(c)" +
            ", (b)-[{w: %f}]->(a)" +
            ", (c)-[{w: %f}]->(a)"
            , 2 / 3.0
            , 1 / 3.0
            , 2 / 3.0
            , 1 / 3.0
        );
    }

    @Test
    void testMutateYields() {
        loadGraph("graph");

        String query = GdsCypher.call()
            .explicitCreation("graph")
            .algo("nodeSimilarity")
            .mutateMode()
            .addParameter("similarityCutoff", 0.0)
            .addParameter("mutateRelationshipType", mutateRelationshipType())
            .addParameter("mutateProperty", mutateProperty())
            .yields(
                "computeMillis",
                "createMillis",
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
            assertThat("Missing createMillis", -1L, lessThan(row.getNumber("createMillis").longValue()));
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

        var graphCreateQuery = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("LIKES", Orientation.UNDIRECTED)
            .graphCreate(graphName)
            .yields();

        runQuery(graphCreateQuery);

        var query = GdsCypher.call()
            .explicitCreation(graphName)
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
}
