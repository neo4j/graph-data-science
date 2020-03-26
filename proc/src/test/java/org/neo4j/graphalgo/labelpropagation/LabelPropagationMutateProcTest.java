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
package org.neo4j.graphalgo.labelpropagation;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.functions.NodePropertyFunc;

import java.util.Arrays;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LabelPropagationMutateProcTest extends LabelPropagationProcTest<LabelPropagationMutateConfig> implements GraphMutationTest<LabelPropagationMutateConfig, LabelPropagation> {

    @Override
    public String mutateProperty() {
        return "communityId";
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a { communityId: 2 }) " +
            ", (b { communityId: 7 }) " +
            ", (a)-->({ communityId: 2 }) " +
            ", (a)-->({ communityId: 3 }) " +
            ", (a)-->({ communityId: 4 }) " +
            ", (a)-->({ communityId: 5 }) " +
            ", (a)-->({ communityId: 6 }) " +
            ", (b)-->({ communityId: 7 }) " +
            ", (b)-->({ communityId: 8 }) " +
            ", (b)-->({ communityId: 9 }) " +
            ", (b)-->({ communityId: 10 }) " +
            ", (b)-->({ communityId: 11 })";
    }

    private static String DB_CYPHER_FILTERED = "CREATE (x:Ignore {id: -1, communityId: null}) " + DB_CYPHER;

    @Override
    public Class<? extends AlgoBaseProc<?, LabelPropagation, LabelPropagationMutateConfig>> getProcedureClazz() {
        return LabelPropagationMutateProc.class;
    }

    @Override
    public LabelPropagationMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return LabelPropagationMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("labelPropagation")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields(
                "createMillis",
                "computeMillis",
                "mutateMillis",
                "postProcessingMillis",
                "communityCount",
                "didConverge",
                "communityDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("mutateMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("postProcessingMillis").longValue()));

                assertEquals(10L, row.getNumber("communityCount"));
                assertTrue(row.getBoolean("didConverge"));

                assertEquals(MapUtil.map(
                    "p99", 2L,
                    "min", 1L,
                    "max", 2L,
                    "mean", 1.2D,
                    "p90", 2L,
                    "p50", 1L,
                    "p999", 2L,
                    "p95", 2L,
                    "p75", 1L
                ), row.get("communityDistribution"));
            }
        );
    }

    @Test
    void testGraphMutationFiltered() throws Exception {
        GraphStoreCatalog.removeAllLoadedGraphs();
        setupGraph(DB_CYPHER_FILTERED);
        registerFunctions(NodePropertyFunc.class);


        String graphName = "loadGraph";

        String loadQuery = GdsCypher
            .call()
            .withNodeLabels("Ignore", "A", "B")
            .withAnyRelationshipType()
            .graphCreate(graphName)
            .yields();

        runQuery(loadQuery);

        String query = GdsCypher
            .call()
            .explicitCreation(graphName)
            .algo("labelPropagation")
            .mutateMode()
            .addParameter("nodeLabels", Arrays.asList("A", "B"))
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQuery(query);

        double[] expectedValues = new double[] {Double.NaN, 3, 8, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, graphName).getGraph();
        mutatedGraph.forEachNode(nodeId -> {
                assertEquals(
                    mutatedGraph.nodeProperties("communityId").nodeProperty(nodeId),
                    expectedValues[Math.toIntExact(nodeId)]
                );
                return true;
            }
        );
    }

}
