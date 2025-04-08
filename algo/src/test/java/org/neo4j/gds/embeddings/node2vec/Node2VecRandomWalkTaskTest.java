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
package org.neo4j.gds.embeddings.node2vec;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.NextNodeSupplier;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.gds.core.compression.common.VarLongEncoding.zigZag;

@GdlExtension
class Node2VecRandomWalkTaskTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:Node1)" +
            ", (b:Node1)" +
            ", (c:Node2)" +
            ", (d:Isolated)" +
            ", (e:Isolated)" +
            ", (a)-[:REL {prop: 1.0}]->(b)" +
            ", (b)-[:REL {prop: 1.0}]->(a)" +
            ", (a)-[:REL {prop: 1.0}]->(c)" +
            ", (c)-[:REL {prop: 1.0}]->(a)" +
            ", (b)-[:REL {prop: 1.0}]->(c)" +
            ", (c)-[:REL {prop: 1.0}]->(b)";

    @Inject
    private Graph graph;


    @Test
    void shouldTriggerCorrectConsumeCalls(){
        NextNodeSupplier.ListNodeSupplier nextNodeSupplier = NextNodeSupplier.ListNodeSupplier.of(
            List.of(graph.toOriginalNodeId(0L),graph.toOriginalNodeId(2L)),
            graph
        );

        var walks = new CompressedRandomWalks(20);

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            graph.nodeCount(),
            new Concurrency(1),
            0.5,
            0.2
        );

        AtomicLong walkIndex = new AtomicLong();
        var task = spy(new Node2VecRandomWalkTask(
            graph,
            nextNodeSupplier,
            10,
            graph::degree,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE,
            walkIndex,
            walks,
            probabilitiesBuilder,
            2,
            42,
            3,
            0.5,
            0.5
        ));

        task.run();

        assertThat(task.maxWalkLength()).isEqualTo(3);
        assertThat(task.maxIndex()).isEqualTo(19L);
        assertThat(walkIndex.longValue()).isEqualTo(20L);

        verify(task,times(20)).consumePath(any(long[].class));
        for (long i=0;i<10;++i) {
            verify(task, times(1)).addPath(
                ArgumentMatchers.same(i),
                ArgumentMatchers.assertArg(path-> assertThat(path[0]).isEqualTo(0L)));
        }
        for (long i=0;i<10;++i) {
            verify(task, times(1)).addPath(
                ArgumentMatchers.same(i + 10L),
                ArgumentMatchers.assertArg(path-> assertThat(path[0]).isEqualTo(zigZag(2L))));
        }
    }

    @Test
    void shouldConsumePathCorrectly(){
        var walks = new CompressedRandomWalks(20);

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            graph.nodeCount(),
            new Concurrency(1),
            0.5,
            0.2
        );

        AtomicLong walkIndex = new AtomicLong();
        var task = new Node2VecRandomWalkTask(
            graph,
            null,
            10,
            graph::degree,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE,
            walkIndex,
            walks,
            probabilitiesBuilder,
            2,
            42,
            3,
            0.5,
            0.5
        );

        task.consumePath(new long[]{0,1,2,1});

        var probs = probabilitiesBuilder.build();

        assertThat(probs.nodeFrequencies().get(0)).isEqualTo(1L);
        assertThat(probs.nodeFrequencies().get(1)).isEqualTo(2L);
        assertThat(probs.nodeFrequencies().get(2)).isEqualTo(1L);
        assertThat(probs.sampleCount()).isEqualTo(4L);
        walks.setSize(1L);
        walks.setMaxWalkLength(4);
        var walk = walks.iterator(0,1).next();

        assertThat(walk).containsExactly(0L,1L,2L,1L);

    }

}
