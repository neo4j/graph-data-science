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
package org.neo4j.gds.hits;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.GdsTestLog;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class HitsTest {

    @GdlGraph(indexInverse = true)
    private static final String GDL =
        "(a), (b), (c), (d), (e), (f), (g), (h)" +
        ", (a)-->(d)" +
        ", (b)-->(c)" +
        ", (b)-->(e)" +
        ", (c)-->(a)" +
        ", (d)-->(c)" +
        ", (e)-->(d)" +
        ", (e)-->(b)" +
        ", (e)-->(f)" +
        ", (e)-->(c)" +
        ", (f)-->(c)" +
        ", (f)-->(h)" +
        ", (g)-->(a)" +
        ", (g)-->(c)" +
        ", (h)-->(a)";

    @Inject
    private TestGraph graph;

    @Test
    void testHits() {
        var config = HitsConfigImpl.builder().concurrency(1).hitsIterations(30).build();

        var  hits =new Hits(graph,config,DefaultPool.INSTANCE,ProgressTracker.NULL_TRACKER);

        var result = hits.compute();

        var pseudoCodeHits = new PseudoCodeHits(30);
        pseudoCodeHits.compute();

        var expectedHubScores = new HashMap<String, Double>();
        var expectedAuthScores = new HashMap<String, Double>();

        var actualHubScores = new HashMap<String, Double>();
        var actualAuthScores = new HashMap<String, Double>();

        List.of("a", "b", "c", "d", "e", "f", "g", "h").forEach(node -> {
            var nodeId = graph.toMappedNodeId(node);

            var expectedHub = pseudoCodeHits.hubs[(int) nodeId];
            var expectedAuth = pseudoCodeHits.auths[(int) nodeId];
            expectedHubScores.put(node, expectedHub);
            expectedAuthScores.put(node, expectedAuth);

            var actualHub = result.nodeValues().doubleProperties(config.hubProperty()).get(nodeId);
            var actualAuth = result.nodeValues().doubleProperties(config.authProperty()).get(nodeId);
            actualHubScores.put(node, actualHub);
            actualAuthScores.put(node, actualAuth);
        });

        assertThat(actualHubScores).containsExactlyEntriesOf(expectedHubScores);
        assertThat(actualAuthScores).containsExactlyEntriesOf(expectedAuthScores);
    }

    @Test
    void shouldLogProgress(){

        var config = HitsConfigImpl.builder().concurrency(1).hitsIterations(5).build();
        var progressTask = HitsProgressTrackerCreator.progressTask(graph.nodeCount(),config.maxIterations(),"Hits");
        var log = new GdsTestLog();
        var progressTracker = new TaskProgressTracker(progressTask, log, new Concurrency(1), EmptyTaskRegistryFactory.INSTANCE);

        var hitsp =new Hits(graph,config,DefaultPool.INSTANCE,progressTracker);
        hitsp.compute();
        Assertions.assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "Hits :: Start",
                "Hits :: Compute iteration 1 of 20 :: Start",
                "Hits :: Compute iteration 1 of 20 100%",
                "Hits :: Compute iteration 1 of 20 :: Finished",
                "Hits :: Master compute iteration 1 of 20 :: Start",
                "Hits :: Master compute iteration 1 of 20 100%",
                "Hits :: Master compute iteration 1 of 20 :: Finished",
                "Hits :: Compute iteration 2 of 20 :: Start",
                "Hits :: Compute iteration 2 of 20 100%",
                "Hits :: Compute iteration 2 of 20 :: Finished",
                "Hits :: Master compute iteration 2 of 20 :: Start",
                "Hits :: Master compute iteration 2 of 20 100%",
                "Hits :: Master compute iteration 2 of 20 :: Finished",
                "Hits :: Compute iteration 3 of 20 :: Start",
                "Hits :: Compute iteration 3 of 20 100%",
                "Hits :: Compute iteration 3 of 20 :: Finished",
                "Hits :: Master compute iteration 3 of 20 :: Start",
                "Hits :: Master compute iteration 3 of 20 100%",
                "Hits :: Master compute iteration 3 of 20 :: Finished",
                "Hits :: Compute iteration 4 of 20 :: Start",
                "Hits :: Compute iteration 4 of 20 100%",
                "Hits :: Compute iteration 4 of 20 :: Finished",
                "Hits :: Master compute iteration 4 of 20 :: Start",
                "Hits :: Master compute iteration 4 of 20 100%",
                "Hits :: Master compute iteration 4 of 20 :: Finished",
                "Hits :: Compute iteration 5 of 20 :: Start",
                "Hits :: Compute iteration 5 of 20 100%",
                "Hits :: Compute iteration 5 of 20 :: Finished",
                "Hits :: Master compute iteration 5 of 20 :: Start",
                "Hits :: Master compute iteration 5 of 20 100%",
                "Hits :: Master compute iteration 5 of 20 :: Finished",
                "Hits :: Compute iteration 6 of 20 :: Start",
                "Hits :: Compute iteration 6 of 20 100%",
                "Hits :: Compute iteration 6 of 20 :: Finished",
                "Hits :: Master compute iteration 6 of 20 :: Start",
                "Hits :: Master compute iteration 6 of 20 100%",
                "Hits :: Master compute iteration 6 of 20 :: Finished",
                "Hits :: Compute iteration 7 of 20 :: Start",
                "Hits :: Compute iteration 7 of 20 100%",
                "Hits :: Compute iteration 7 of 20 :: Finished",
                "Hits :: Master compute iteration 7 of 20 :: Start",
                "Hits :: Master compute iteration 7 of 20 100%",
                "Hits :: Master compute iteration 7 of 20 :: Finished",
                "Hits :: Compute iteration 8 of 20 :: Start",
                "Hits :: Compute iteration 8 of 20 100%",
                "Hits :: Compute iteration 8 of 20 :: Finished",
                "Hits :: Master compute iteration 8 of 20 :: Start",
                "Hits :: Master compute iteration 8 of 20 100%",
                "Hits :: Master compute iteration 8 of 20 :: Finished",
                "Hits :: Compute iteration 9 of 20 :: Start",
                "Hits :: Compute iteration 9 of 20 100%",
                "Hits :: Compute iteration 9 of 20 :: Finished",
                "Hits :: Master compute iteration 9 of 20 :: Start",
                "Hits :: Master compute iteration 9 of 20 100%",
                "Hits :: Master compute iteration 9 of 20 :: Finished",
                "Hits :: Compute iteration 10 of 20 :: Start",
                "Hits :: Compute iteration 10 of 20 100%",
                "Hits :: Compute iteration 10 of 20 :: Finished",
                "Hits :: Master compute iteration 10 of 20 :: Start",
                "Hits :: Master compute iteration 10 of 20 100%",
                "Hits :: Master compute iteration 10 of 20 :: Finished",
                "Hits :: Compute iteration 11 of 20 :: Start",
                "Hits :: Compute iteration 11 of 20 100%",
                "Hits :: Compute iteration 11 of 20 :: Finished",
                "Hits :: Master compute iteration 11 of 20 :: Start",
                "Hits :: Master compute iteration 11 of 20 100%",
                "Hits :: Master compute iteration 11 of 20 :: Finished",
                "Hits :: Compute iteration 12 of 20 :: Start",
                "Hits :: Compute iteration 12 of 20 100%",
                "Hits :: Compute iteration 12 of 20 :: Finished",
                "Hits :: Master compute iteration 12 of 20 :: Start",
                "Hits :: Master compute iteration 12 of 20 100%",
                "Hits :: Master compute iteration 12 of 20 :: Finished",
                "Hits :: Compute iteration 13 of 20 :: Start",
                "Hits :: Compute iteration 13 of 20 100%",
                "Hits :: Compute iteration 13 of 20 :: Finished",
                "Hits :: Master compute iteration 13 of 20 :: Start",
                "Hits :: Master compute iteration 13 of 20 100%",
                "Hits :: Master compute iteration 13 of 20 :: Finished",
                "Hits :: Compute iteration 14 of 20 :: Start",
                "Hits :: Compute iteration 14 of 20 100%",
                "Hits :: Compute iteration 14 of 20 :: Finished",
                "Hits :: Master compute iteration 14 of 20 :: Start",
                "Hits :: Master compute iteration 14 of 20 100%",
                "Hits :: Master compute iteration 14 of 20 :: Finished",
                "Hits :: Compute iteration 15 of 20 :: Start",
                "Hits :: Compute iteration 15 of 20 100%",
                "Hits :: Compute iteration 15 of 20 :: Finished",
                "Hits :: Master compute iteration 15 of 20 :: Start",
                "Hits :: Master compute iteration 15 of 20 100%",
                "Hits :: Master compute iteration 15 of 20 :: Finished",
                "Hits :: Compute iteration 16 of 20 :: Start",
                "Hits :: Compute iteration 16 of 20 100%",
                "Hits :: Compute iteration 16 of 20 :: Finished",
                "Hits :: Master compute iteration 16 of 20 :: Start",
                "Hits :: Master compute iteration 16 of 20 100%",
                "Hits :: Master compute iteration 16 of 20 :: Finished",
                "Hits :: Compute iteration 17 of 20 :: Start",
                "Hits :: Compute iteration 17 of 20 100%",
                "Hits :: Compute iteration 17 of 20 :: Finished",
                "Hits :: Master compute iteration 17 of 20 :: Start",
                "Hits :: Master compute iteration 17 of 20 100%",
                "Hits :: Master compute iteration 17 of 20 :: Finished",
                "Hits :: Compute iteration 18 of 20 :: Start",
                "Hits :: Compute iteration 18 of 20 100%",
                "Hits :: Compute iteration 18 of 20 :: Finished",
                "Hits :: Master compute iteration 18 of 20 :: Start",
                "Hits :: Master compute iteration 18 of 20 100%",
                "Hits :: Master compute iteration 18 of 20 :: Finished",
                "Hits :: Compute iteration 19 of 20 :: Start",
                "Hits :: Compute iteration 19 of 20 100%",
                "Hits :: Compute iteration 19 of 20 :: Finished",
                "Hits :: Master compute iteration 19 of 20 :: Start",
                "Hits :: Master compute iteration 19 of 20 100%",
                "Hits :: Master compute iteration 19 of 20 :: Finished",
                "Hits :: Compute iteration 20 of 20 :: Start",
                "Hits :: Compute iteration 20 of 20 100%",
                "Hits :: Compute iteration 20 of 20 :: Finished",
                "Hits :: Master compute iteration 20 of 20 :: Start",
                "Hits :: Master compute iteration 20 of 20 100%",
                "Hits :: Master compute iteration 20 of 20 :: Finished",
                "Hits :: Finished"
            );
    }

    // Basically implements the pseudo code from Wikipedia
    private class PseudoCodeHits {
        final double[] auths;
        final double[] hubs;

        private final int k;
        private double norm;

        PseudoCodeHits(int k) {
            this.k = k;
            auths = new double[(int) graph.nodeCount()];
            hubs = new double[(int) graph.nodeCount()];
            norm = 0;
        }

        void compute() {
            for (int i = 0; i < graph.nodeCount(); i++) {
                auths[i] = 1.0D;
                hubs[i] = 1.0D;
            }

            for (int i = 0; i < k; i++) {
                norm = 0;
                for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                    auths[nodeId] = 0.0D;
                    graph.forEachInverseRelationship(nodeId, (s, t) -> {
                        auths[(int) s] += hubs[(int) t];
                        return true;
                    });
                    norm += Math.pow(auths[nodeId], 2);
                }
                norm = Math.sqrt(norm);
                for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                    auths[nodeId] = auths[nodeId] / norm;
                }

                norm = 0;
                for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                    hubs[nodeId] = 0.0D;
                    graph.forEachRelationship(nodeId, (s, t) -> {
                        hubs[(int) s] += auths[(int) t];
                        return true;
                    });
                    norm += Math.pow(hubs[nodeId], 2);
                }
                norm = Math.sqrt(norm);
                for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                    hubs[nodeId] = hubs[nodeId] / norm;
                }
            }
        }
    }
}
