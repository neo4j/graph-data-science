/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.nodesim;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NodeSimilarityMutateProcTest
    extends NodeSimilarityProcTest<NodeSimilarityMutateConfig>
    implements GraphMutationTest<NodeSimilarityMutateConfig, NodeSimilarityResult> {

    private static final String MUTATE_RELATIONSHIP_TYPE = "SIMILAR_TO";

    @Override
    public String mutateProperty() {
        return "similarity";
    }

    @Override
    public Class<? extends AlgoBaseProc<?, NodeSimilarityResult, NodeSimilarityMutateConfig>> getProcedureClazz() {
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
            mapWrapper = mapWrapper.withString("mutateRelationshipType", MUTATE_RELATIONSHIP_TYPE);
        }
        return mapWrapper;
    }

    @Override
    public String expectedMutatedGraph() {
        return String.format(
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

    @Override
    public String failOnExistingTokenMessage() {
        return String.format(
            "Relationship type `%s` already exists in the in-memory graph.",
            MUTATE_RELATIONSHIP_TYPE
        );
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("nodeSimilarity")
            .mutateMode()
            .addParameter("similarityCutoff", 0.0)
            .addParameter("mutateRelationshipType", MUTATE_RELATIONSHIP_TYPE)
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
}
