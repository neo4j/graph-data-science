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
package org.neo4j.graphalgo.impl.wcc;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.CommunityHelper;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.ImmutableModernGraphLoader;
import org.neo4j.graphalgo.core.ModernGraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateFromStoreConfig;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.core.utils.ParallelUtil.DEFAULT_BATCH_SIZE;

public class WccThresholdTest extends AlgoTestBase {

    @BeforeEach
    void setup() {
        @Language("Cypher") String cypher =
            "CREATE" +
            " (nA:Label {nodeId: 0, seedId: 42})" +
            ",(nB:Label {nodeId: 1, seedId: 42})" +
            ",(nC:Label {nodeId: 2, seedId: 42})" +
            ",(nD:Label {nodeId: 3, seedId: 42})" +
            ",(nE {nodeId: 4})" +
            ",(nF {nodeId: 5})" +
            ",(nG {nodeId: 6})" +
            ",(nH {nodeId: 7})" +
            ",(nI {nodeId: 8})" +
            ",(nJ {nodeId: 9})" +
            // {A, B, C, D}
            ",(nA)-[:TYPE]->(nB)" +
            ",(nB)-[:TYPE]->(nC)" +
            ",(nC)-[:TYPE]->(nD)" +
            ",(nD)-[:TYPE {cost:4.2}]->(nE)" + // threshold UF should split here
            // {E, F, G}
            ",(nE)-[:TYPE]->(nF)" +
            ",(nF)-[:TYPE]->(nG)" +
            // {H, I}
            ",(nH)-[:TYPE]->(nI)";
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(cypher);
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.impl.wcc.WccThresholdTest#thresholdParams")
    void testThreshold(double threshold, long[][] expectedComponents) {
        GraphCreateConfig createConfig = ImmutableGraphCreateFromStoreConfig.builder()
            .graphName("myGraph")
            .nodeProjection(NodeProjections.empty())
            .relationshipProjection(
                RelationshipProjections.builder()
                    .putProjection(
                        ElementIdentifier.of("TYPE"),
                        RelationshipProjection.builder()
                            .type("TYPE")
                            .properties(
                                PropertyMappings.of(
                                    PropertyMapping.of("cost", 10.0)
                                )
                            ).build()
                    )
                    .build())
            .build();

        WccStreamConfig wccConfig = ImmutableWccStreamConfig
            .builder()
            .threshold(threshold)
            .weightProperty("cost")
            .implicitCreateConfig(createConfig)
            .build();

        ModernGraphLoader loader = ImmutableModernGraphLoader
            .builder()
            .api(db)
            .username("")
            .log(new TestLog())
            .createConfig(createConfig).build();

        Graph graph = loader.load(HugeGraphFactory.class);

        DisjointSetStruct dss = new Wcc(
            graph,
            Pools.DEFAULT,
            DEFAULT_BATCH_SIZE,
            wccConfig,
            AllocationTracker.EMPTY
        ).compute();

        long[] communityData = new long[(int) graph.nodeCount()];
        graph.forEachNode(nodeId -> {
            communityData[(int) nodeId] = dss.setIdOf(nodeId);
            return true;
        });

        CommunityHelper.assertCommunities(communityData, expectedComponents);
    }

    static Stream<Arguments> thresholdParams() {
        return Stream.of(
            arguments(5.0, new long[][]{new long[]{0L, 1L, 2L, 3L}, new long[]{4, 5, 6}, new long[]{7, 8}, new long[]{9}}),
            arguments(3.14, new long[][]{new long[]{0L, 1L, 2L, 3L, 4, 5, 6}, new long[]{7, 8}, new long[]{9}})
        );
    }
}
