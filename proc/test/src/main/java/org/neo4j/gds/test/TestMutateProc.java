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
package org.neo4j.gds.test;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.LongNodeProperties;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class TestMutateProc extends MutatePropertyProc<TestAlgorithm, TestAlgorithm, TestResult, TestMutateConfig> {

    @Procedure(value = "gds.testProc.mutate", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<TestResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<TestAlgorithm, TestAlgorithm, TestMutateConfig> computationResult = compute(
            graphName,
            configuration
        );
        return mutate(computationResult);
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<TestAlgorithm, TestAlgorithm, TestMutateConfig> computationResult) {
        return new LongNodeProperties() {
            @Override
            public long longValue(long nodeId) {
                return nodeId;
            }

            @Override
            public long size() {
                return 0;
            }
        };
    }

    @Override
    protected AbstractResultBuilder<TestResult> resultBuilder(ComputationResult<TestAlgorithm, TestAlgorithm, TestMutateConfig> computeResult) {
        return new TestResult.TestResultBuilder().withRelationshipCount(computeResult.result().relationshipCount());
    }

    @Override
    protected TestMutateConfig newConfig(String username, CypherMapWrapper config) {
        return TestMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<TestAlgorithm, TestMutateConfig> algorithmFactory() {
        return new GraphAlgorithmFactory<>() {

            @Override
            public String taskName() {
                return "TestAlgorithm";
            }

            @Override
            public TestAlgorithm build(
                Graph graph,
                TestMutateConfig configuration,
                AllocationTracker allocationTracker,
                ProgressTracker progressTracker
            ) {
                return new TestAlgorithm(
                    graph,
                    allocationTracker,
                    0L,
                    progressTracker,
                    configuration.throwInCompute()
                );
            }
        };
    }
}
