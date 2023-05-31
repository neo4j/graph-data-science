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

import org.apache.commons.lang3.mutable.MutableInt;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.CommunityHelper.assertCommunities;
import static org.neo4j.gds.GdsCypher.ExecutionModes.WRITE;

class ModularityOptimizationWriteProcTest extends BaseProcTest {

    private static final String COMMUNITY = "community";
    private static final String COMMUNITY_COUNT = "communityCount";

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE" +
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


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ModularityOptimizationWriteProc.class,
            GraphProjectProc.class
        );

        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeProperty("seed1")
            .withRelationshipProperty("weight")
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testWriting() {
        var createQuery = GdsCypher.call("naturalGraph")
            .graphProject()
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(createQuery);
        String query = GdsCypher.call("naturalGraph")
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getBoolean("didConverge")).isTrue();
            assertThat(row.getNumber("modularity"))
                .asInstanceOf(DOUBLE)
                // this value changed after adapting to OUTGOING
                .isEqualTo(-0.0408, Offset.offset(0.001));
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isLessThanOrEqualTo(3);
        });

        assertWriteResult(new long[]{0, 1, 2, 4}, new long[]{3, 5});
    }
    @Test
    void testWritingUndirected() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getBoolean("didConverge")).isTrue();
            assertThat(row.getNumber("modularity"))
                .asInstanceOf(DOUBLE)
                .isEqualTo(0.12244, Offset.offset(0.001));
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isLessThanOrEqualTo(3);
        });

        assertWriteResult(new long[]{0, 1, 2, 4}, new long[]{3, 5});
    }

    @Test
    void testWritingWeighted() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getBoolean("didConverge")).isTrue();
            assertThat(row.getNumber("modularity"))
                .asInstanceOf(DOUBLE)
                .isEqualTo(0.4985, Offset.offset(0.001));
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isLessThanOrEqualTo(3);
        });

        assertWriteResult(new long[]{0, 4, 5}, new long[]{1, 2, 3});
    }

    @Test
    void testWritingSeeded() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("seedProperty", "seed1")
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQuery(query);

        long[] communities = new long[6];
        MutableInt i = new MutableInt(0);
        runQueryWithRowConsumer("MATCH (n) RETURN n.community as community", (row) -> {
            communities[i.getAndIncrement()] = row.getNumber(COMMUNITY).longValue();
        });
        assertCommunities(communities, new long[]{0, 1}, new long[]{2, 3, 4, 5});
    }

    @Test
    void testWritingTolerance() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("tolerance", 1)
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertThat(row.getBoolean("didConverge")).isTrue();

            // Cannot converge after one iteration,
            // because it doesn't have anything to compare the computed modularity against.
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
        });
    }

    @Test
    void testWritingIterations() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("maxIterations", 1)
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertThat(row.getBoolean("didConverge")).isFalse();
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isEqualTo(1);
        });
    }

    @Test
    void testWritingEstimate() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .estimationMode(WRITE)
            .addParameter("writeProperty", COMMUNITY)
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
                Arguments.of(Map.of("minCommunitySize", 1), List.of(5L, 4L)),
                Arguments.of(Map.of("minCommunitySize", 3), List.of(4L)),
                Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), List.of(0L, 1L)),
                Arguments.of(Map.of("minCommunitySize", 3, "consecutiveIds", true), List.of(0L))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteMinCommunitySize(Map<String, Object> parameters, List<Long> expectedCommunityIds) {
        var query = GdsCypher
                .call(DEFAULT_GRAPH_NAME)
                .algo("gds", "beta", "modularityOptimization")
                .writeMode()
                .addParameter("writeProperty", COMMUNITY)
                .addAllParameters(parameters)
                .yields(COMMUNITY_COUNT);

        runQueryWithRowConsumer(query, row -> assertThat(row.getNumber(COMMUNITY_COUNT)).isEqualTo(2L));

        runQueryWithRowConsumer(
                "MATCH (n) RETURN collect(DISTINCT n." + COMMUNITY + ") AS communities ",
                row -> assertThat(row.get("communities"))
                        .asList()
                        .containsExactlyInAnyOrderElementsOf(expectedCommunityIds)
        );
    }

    private void assertWriteResult(long[]... expectedCommunities) {
        Map<String, Object> nameMapping = MapUtil.map(
            "a", 0,
            "b", 1,
            "c", 2,
            "d", 3,
            "e", 4,
            "f", 5
        );
        long[] actualCommunities = new long[6];
        runQueryWithRowConsumer("MATCH (n) RETURN n.name as name, n.community as community", (row) -> {
            long community = row.getNumber(COMMUNITY).longValue();
            String name = row.getString("name");
            actualCommunities[(int) nameMapping.get(name)] = community;
        });

        assertCommunities(actualCommunities, expectedCommunities);
    }
}
