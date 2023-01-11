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
package org.neo4j.gds.pregel;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Inject
    private IdFunction idFunction;

    @Test
    void testHits() {
        var config = ImmutableHitsConfig.builder().concurrency(1).hitsIterations(30).build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new Hits(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = pregelJob.run();

        var pseudoCodeHits = new PseudoCodeHits(30);
        pseudoCodeHits.compute();

        var expectedHubScores = new HashMap<String, Double>();
        var expectedAuthScores = new HashMap<String, Double>();

        var actualHubScores = new HashMap<String, Double>();
        var actualAuthScores = new HashMap<String, Double>();

        List.of("a", "b", "c", "d", "e", "f", "g", "h").forEach(node -> {
            var nodeId = idFunction.of(node);

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
