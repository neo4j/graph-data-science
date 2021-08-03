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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;

import static org.neo4j.graphalgo.TestSupport.assertAlgorithmTermination;

class NodeSimilarityTerminationTest extends AlgoTestBase {

    @Timeout(value = 10)
    @Test
    void shouldTerminate() {
        HugeGraph graph = RandomGraphGenerator.builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .allocationTracker(AllocationTracker.empty())
            .build()
            .generate();

        NodeSimilarity nodeSimilarity = new NodeSimilarity(
            graph,
            NodeSimilarityTest.configBuilder().concurrency(1).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        );

        assertAlgorithmTermination(
            db,
            nodeSimilarity,
            nhs -> nodeSimilarity.computeToStream(),
            100
        );
    }
}
