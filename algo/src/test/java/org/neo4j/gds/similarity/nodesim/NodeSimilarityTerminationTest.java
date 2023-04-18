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
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import static org.neo4j.gds.graphbuilder.TransactionTerminationTestUtils.assertTerminates;

class NodeSimilarityTerminationTest extends BaseTest {

    @Test
    void shouldTerminate() {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(100_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .build()
            .generate();

        assertTerminates((terminationFlag) ->
            {
                var nodeSimilarity = NodeSimilarity.create(
                    graph,
                    NodeSimilarityTest.configBuilder().concurrency(1).build(),
                    Pools.DEFAULT,
                    ProgressTracker.NULL_TRACKER
                );
                nodeSimilarity.setTerminationFlag(terminationFlag);
                nodeSimilarity.compute();
            }, 500, 1000
        );
    }
}
