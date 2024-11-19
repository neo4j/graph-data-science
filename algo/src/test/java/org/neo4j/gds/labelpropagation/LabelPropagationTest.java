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
package org.neo4j.gds.labelpropagation;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@GdlExtension
@ExtendWith(SoftAssertionsExtension.class)
class LabelPropagationTest {

    private static final LabelPropagationParameters DEFAULT_PARAMETERS = new LabelPropagationParameters(
        new Concurrency(4),
        10,
        null,
        null
    );

    // override idOffset for seedId to be actual neo4j ids
    @GdlGraph
    private static final String GRAPH =
        "CREATE" +
        "  (nAlice:User   {seedId: 2})" +
        ", (nBridget:User {seedId: 3})" +
        ", (nCharles:User {seedId: 4})" +
        ", (nDoug:User    {seedId: 3})" +
        ", (nMark:User    {seedId: 4})" +
        ", (nMichael:User {seedId: 2})" +
        ", (nAlice)-[:FOLLOW]->(nBridget)" +
        ", (nAlice)-[:FOLLOW]->(nCharles)" +
        ", (nMark)-[:FOLLOW]->(nDoug)" +
        ", (nBridget)-[:FOLLOW]->(nMichael)" +
        ", (nDoug)-[:FOLLOW]->(nMark)" +
        ", (nMichael)-[:FOLLOW]->(nAlice)" +
        ", (nAlice)-[:FOLLOW]->(nMichael)" +
        ", (nBridget)-[:FOLLOW]->(nAlice)" +
        ", (nMichael)-[:FOLLOW]->(nBridget)" +
        ", (nCharles)-[:FOLLOW]->(nDoug)";

    @Inject
    private TestGraph graph;

    @InjectSoftAssertions
    SoftAssertions soft;

    @Test
    void shouldUseOriginalNodeIdWhenSeedPropertyIsMissing() {
        LabelPropagation lp = new LabelPropagation(
            graph,
            new LabelPropagationParameters(new Concurrency(4), 1, null, null),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );
        assertArrayEquals(
            new long[]{
                graph.toOriginalNodeId("nBridget"),
                graph.toOriginalNodeId("nBridget"),
                graph.toOriginalNodeId("nDoug"),
                graph.toOriginalNodeId("nMark"),
                graph.toOriginalNodeId("nMark"),
                graph.toOriginalNodeId("nBridget")
            },
            lp.compute().labels().toArray(),
            "Incorrect result assuming initial labels are neo4j id"
        );
    }

    @Test
    void shouldUseSeedProperty() {
        LabelPropagation lp = new LabelPropagation(
            graph,
            new LabelPropagationParameters(new Concurrency(4), 1, null, "seedId"),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        assertArrayEquals(
            new long[]{
                graph.toMappedNodeId("nCharles"),
                graph.toMappedNodeId("nCharles"),
                graph.toMappedNodeId("nDoug"),
                graph.toMappedNodeId("nMark"),
                graph.toMappedNodeId("nMark"),
                graph.toMappedNodeId("nCharles")
            },
            lp.compute().labels().toArray()
        );
    }

    @Test
    void testSingleThreadClustering() {
        testClustering(graph, 100);
    }

    @Test
    void testMultiThreadClustering() {
        testClustering(graph, 2);
    }

    private void testClustering(TestGraph graph, int batchSize) {
        for (int i = 0; i < 20; i++) {
            testLPClustering(graph, batchSize);
        }
    }

    private void testLPClustering(TestGraph graph, int batchSize) {
        LabelPropagation lp = new LabelPropagation(
            graph,
            DEFAULT_PARAMETERS,
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );
        lp.withBatchSize(batchSize);

        var result = lp.compute();

        var labels = result.labels();
        assertThat(labels).isNotNull();

       var cluster = groupByPartitionInt(labels);

        assertThat(result.didConverge())
            .as("The algorithms should have converged.")
            .isTrue();

        assertThat(result.ranIterations())
            .as("Expected at least two iterations")
            .isGreaterThanOrEqualTo(2);

        assertThat(cluster)
            .as("Expected exactly two clusters")
            .hasSize(2);

        var firstCommunity = Stream.of("nAlice", "nBridget", "nMichael")
            .map(graph::toOriginalNodeId)
            .collect(Collectors.toSet());

        for (IntObjectCursor<IntArrayList> cursor : cluster) {
            int[] ids = cursor.value.toArray();
            if (firstCommunity.contains((long) cursor.key)) {
                soft.assertThat(ids).containsExactlyInAnyOrder(0, 1, 5);
            } else {
                soft.assertThat(ids).containsExactlyInAnyOrder(2, 3, 4);
            }
        }
    }

    private static IntObjectMap<IntArrayList> groupByPartitionInt(HugeLongArray labels) {
        if (labels == null) {
            return null;
        }
        IntObjectMap<IntArrayList> cluster = new IntObjectHashMap<>();
        for (int node = 0, l = Math.toIntExact(labels.size()); node < l; node++) {
            int key = Math.toIntExact(labels.get(node));
            IntArrayList ids = cluster.get(key);
            if (ids == null) {
                ids = new IntArrayList();
                cluster.put(key, ids);
            }
            ids.add(node);
        }

        return cluster;
    }
}
