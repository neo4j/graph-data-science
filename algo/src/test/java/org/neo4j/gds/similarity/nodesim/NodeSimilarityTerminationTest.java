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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.wcc.WccStub;

import static org.neo4j.gds.graphbuilder.TransactionTerminationTestUtils.assertTerminates;

class NodeSimilarityTerminationTest {

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
                var parameters = new NodeSimilarityParameters(
                    new JaccardSimilarityComputer(1E-42),
                    1,
                    Integer.MAX_VALUE,
                    10,
                    0,
                    true,
                    false,
                    false,
                    null
                );
                var nodeSimilarity = new NodeSimilarity(
                    graph,
                    parameters,
                    new Concurrency(1),
                    DefaultPool.INSTANCE,
                    ProgressTracker.NULL_TRACKER,
                    NodeFilter.ALLOW_EVERYTHING,
                    NodeFilter.ALLOW_EVERYTHING,
                    TerminationFlag.RUNNING_TRUE,
                    new WccStub(TerminationFlag.RUNNING_TRUE, new AlgorithmMachinery())
                );
                nodeSimilarity.setTerminationFlag(terminationFlag);
                nodeSimilarity.compute();
            }, 500, 1000
        );
    }
}
