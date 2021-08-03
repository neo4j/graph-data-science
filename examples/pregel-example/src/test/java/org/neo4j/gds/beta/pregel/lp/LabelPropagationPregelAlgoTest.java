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
package org.neo4j.gds.beta.pregel.lp;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.Map;

import static org.neo4j.graphalgo.TestSupport.assertLongValues;

@GdlExtension
class LabelPropagationPregelAlgoTest {

    // https://neo4j.com/blog/graph-algorithms-neo4j-label-propagation/
    //
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String TEST_GRAPH =
            "CREATE" +
            "  (nAlice:User)" +
            ", (nBridget:User)" +
            ", (nCharles:User)" +
            ", (nDoug:User)" +
            ", (nMark:User)" +
            ", (nMichael:User)" +
            ", (nAlice)-[:FOLLOW   {weight: 1.0}]->(nBridget)" +
            ", (nAlice)-[:FOLLOW   {weight: 1.0}]->(nCharles)" +
            ", (nMark)-[:FOLLOW    {weight: 1.0}]->(nDoug)" +
            ", (nBridget)-[:FOLLOW {weight: 1.0}]->(nMichael)" +
            ", (nDoug)-[:FOLLOW    {weight: 2.0}]->(nMark)" +
            ", (nMichael)-[:FOLLOW {weight: 1.0}]->(nAlice)" +
            ", (nAlice)-[:FOLLOW   {weight: 1.0}]->(nMichael)" +
            ", (nBridget)-[:FOLLOW {weight: 1.0}]->(nAlice)" +
            ", (nMichael)-[:FOLLOW {weight: 1.0}]->(nBridget)" +
            ", (nCharles)-[:FOLLOW {weight: 1.0}]->(nDoug)";

    @Inject
    private TestGraph graph;

    @Test
    void runLP() {
        int maxIterations = 10;

        var config = ImmutableLabelPropagationPregelConfig.builder()
            .maxIterations(maxIterations)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new LabelPropagationPregel(),
            Pools.DEFAULT,
            AllocationTracker.empty(),
            ProgressLogger.NULL_LOGGER
        );

        var nodeValues = pregelJob.run()
            .nodeValues()
            .longProperties(LabelPropagationPregel.LABEL_KEY);

        assertLongValues(graph, nodeValues::get, Map.of(
            "nAlice", 0L,
            "nBridget", 0L,
            "nCharles", 0L,
            "nDoug", 4L,
            "nMark", 3L,
            "nMichael", 0L
        ));
    }

    @Test
    void runWeightedLP() {
        int maxIterations = 10;

        var config = ImmutableLabelPropagationPregelConfig.builder()
            .maxIterations(maxIterations)
            .relationshipWeightProperty("weight")
            .build();

        var weightedLabelPropagation = new LabelPropagationPregel() {
            @Override
            public double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
                return nodeValue * relationshipWeight;
            }
        };

        var pregelJob = Pregel.create(
            graph,
            config,
            weightedLabelPropagation,
            Pools.DEFAULT,
            AllocationTracker.empty(),
            ProgressLogger.NULL_LOGGER
        );

        var nodeValues = pregelJob.run()
            .nodeValues()
            .longProperties(LabelPropagationPregel.LABEL_KEY);

        assertLongValues(graph, nodeValues::get, Map.of(
            "nAlice", 0L,
            "nBridget", 0L,
            "nCharles", 0L,
            "nDoug", 0L,
            "nMark", 0L,
            "nMichael", 0L
        ));
    }
}
