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
package org.neo4j.graphalgo.wcc;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.config.ConcurrencyConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class IncrementalWccTest {

    private static final String SEED_PROPERTY = "community";

    private static final int COMMUNITY_COUNT = 16;
    private static final int COMMUNITY_SIZE = 10;

    /**
     * Create multiple communities and connect them pairwise.
     */
    static Graph createGraph() {
        StringBuilder gdl = new StringBuilder();

        for (int i = 0; i < COMMUNITY_COUNT; i = i + 2) {
            gdl.append(createCommunity(i));
            gdl.append(createCommunity(i + 1));
            gdl.append(formatWithLocale(
                "(n_%d_%d)-[:REL]->(n_%d_%d)",
                COMMUNITY_SIZE - 1, i,
                COMMUNITY_SIZE - 1, i + 1
            ));
        }
        return fromGdl(gdl.toString());
    }

    static String createCommunity(int communityId) {
        return IntStream.range(0, COMMUNITY_SIZE)
            .mapToObj(nodeId -> formatWithLocale(
                "(n_%d_%d {%s: %d})",
                nodeId, communityId, SEED_PROPERTY, communityId))
            .collect(Collectors.joining("-[:REL]->"));
    }

    @Test
    void shouldComputeComponentsFromSeedProperty() {
        Graph graph = createGraph();

        WccStreamConfig config = ImmutableWccStreamConfig.builder()
            .concurrency(ConcurrencyConfig.DEFAULT_CONCURRENCY)
            .seedProperty(SEED_PROPERTY)
            .threshold(0D)
            .build();

        // We expect that UF connects pairs of communities
        DisjointSetStruct result = run(graph, config);
        assertEquals(COMMUNITY_COUNT / 2, getSetCount(result));

        graph.forEachNode((nodeId) -> {
            // Since the community size has doubled, the community id first needs to be computed with twice the
            // community size. To account for the gaps in the community ids when communities have merged,
            // we need to multiply the resulting id by two.
            long expectedCommunityId = nodeId / (2 * COMMUNITY_SIZE) * 2;
            long actualCommunityId = result.setIdOf(nodeId);
            assertEquals(
                expectedCommunityId,
                actualCommunityId,
                "Node " + nodeId + " in unexpected set: " + actualCommunityId
            );
            return true;
        });
    }

    @Test
    void shouldAssignMinimumCommunityIdOnMerge() {
        // Given
        // Simulates new node (a) created with lower ID and no seed
        Graph graph = fromGdl(
            "  (a {id: 1, seed: 43})" +
            ", (b {id: 2, seed: 42})" +
            ", (c {id: 3, seed: 42})" +
            ", (a)-->(b)" +
            ", (b)-->(c)"
        );

        // When
        WccStreamConfig config = ImmutableWccStreamConfig.builder()
            .seedProperty("seed")
            .build();

        DisjointSetStruct result = run(graph, config);

        // Then
        LongStream.range(IdMapping.START_NODE_ID, graph.nodeCount())
            .forEach(node -> assertEquals(42, result.setIdOf(node)));
    }

    private DisjointSetStruct run(Graph graph, WccBaseConfig config) {
        return new Wcc(
            graph,
            Pools.DEFAULT,
            COMMUNITY_SIZE / ConcurrencyConfig.DEFAULT_CONCURRENCY,
            config,
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        ).compute();
    }

    /**
     * Compute number of sets present.
     */
    private long getSetCount(DisjointSetStruct struct) {
        long capacity = struct.size();
        BitSet sets = new BitSet(capacity);
        for (long i = 0L; i < capacity; i++) {
            long setId = struct.setIdOf(i);
            sets.set(setId);
        }
        return sets.cardinality();
    }

}
