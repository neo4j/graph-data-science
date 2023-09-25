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
package org.neo4j.gds.modularityoptimization;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.IdToVariable;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.CommunityHelper.assertCommunities;
import static org.neo4j.gds.GdsCypher.ExecutionModes.STREAM;

class ModularityOptimizationStreamProcTest extends BaseProcTest {

    private static final String GRAPH_NAME = "custom-graph";

    @Neo4jGraph
    static final String DB_CYPHER = "CREATE" +
        "  (a:Node {name:'a', seed1: 0, seed2: 1})" +
        ", (b:Node {name:'b', seed1: 0, seed2: 1})" +
        ", (c:Node {name:'c', seed1: 2, seed2: 1})" +
        ", (d:Node {name:'d', seed1: 2, seed2: 42})" +
        ", (e:Node {name:'e', seed1: 2, seed2: 42})" +
        ", (f:Node {name:'f', seed1: 2, seed2: 42})" +
        ", (a)-[:TYPE {weight: 0.01}]->(b)" +
        ", (a)-[:TYPE {weight: 5.0}]->(e)" +
        ", (a)-[:TYPE {weight: 5.0}]->(f)" +
        ", (b)-[:TYPE {weight: 5.0}]->(c)" +
        ", (b)-[:TYPE {weight: 5.0}]->(d)" +
        ", (c)-[:TYPE {weight: 0.01}]->(e)" +
        ", (f)-[:TYPE {weight: 0.01}]->(d)";

    @Inject
    IdToVariable idToVariable;

    @Inject
    IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ModularityOptimizationStreamProc.class,
            GraphProjectProc.class
        );

        var createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withRelationshipProperty("weight")
            .withNodeProperty("seed1")
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStreaming() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("modularityOptimization")
            .streamMode()
            .yields("nodeId", "communityId");

        Map<Long, Long> communities = new HashMap<>(6);
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            communities.put(nodeId, row.getNumber("communityId").longValue());
        });

        assertCommunities(communities, idFunction.of("a", "b", "c", "e"), idFunction.of("d", "f"));
    }

    @Test
    void testStreamingWeighted() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("modularityOptimization")
            .streamMode()
            .addParameter("relationshipWeightProperty", "weight")
            .yields("nodeId", "communityId");

        Map<Long, Long> communities = new HashMap<>(6);
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            communities.put(nodeId, row.getNumber("communityId").longValue());
        });

        assertCommunities(communities, idFunction.of("a", "e", "f"), idFunction.of("b", "c", "d"));
    }

    @Test
    void testStreamingSeeded() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("modularityOptimization")
            .streamMode()
            .addParameter("seedProperty", "seed1")
            .yields("nodeId", "communityId");

        Map<Long, Long> communities = new HashMap<>(6);
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            communities.put(nodeId, row.getNumber("communityId").longValue());
        });

        assertCommunities(communities, idFunction.of("a", "b"), idFunction.of("c",  "d", "e", "f"));
        assertThat(communities.values()).containsExactly(0L, 0L, 2L, 2L, 2L, 2L);
    }

    @Test
    void testStreamingEstimate() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("modularityOptimization")
            .estimationMode(STREAM)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertThat(row.getNumber("bytesMin"))
                .asInstanceOf(LONG)
                .isPositive();
            assertThat(row.getNumber("bytesMax"))
                .asInstanceOf(LONG)
                .isPositive();
        });
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
            Arguments.of(Map.of("minCommunitySize", 1), Set.of(5L, 4L)),
            Arguments.of(Map.of("minCommunitySize", 3), Set.of(4L)),
            Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), Set.of(0L, 1L)),
            Arguments.of(Map.of("minCommunitySize", 3, "consecutiveIds", true), Set.of(0L))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteMinCommunitySize(Map<String, Object> parameters, Iterable<Long> expectedCommunityIds) {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("modularityOptimization")
            .streamMode()
            .addAllParameters(parameters)
            .yields("nodeId", "communityId");

        var actualCommunities = new HashSet<Long>();

        runQueryWithRowConsumer(query, row -> {
            long communityId = row.getNumber("communityId").longValue();
            actualCommunities.add(communityId);
        });

        assertThat(actualCommunities)
            .containsExactlyInAnyOrderElementsOf(expectedCommunityIds);
    }
}
