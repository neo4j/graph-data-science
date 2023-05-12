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
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.test.Constants.STATS_DESCRIPTION;

@GdsCallable(
    name = "gds.testProc.mutate", description = STATS_DESCRIPTION, executionMode = ExecutionMode.MUTATE_NODE_PROPERTY
)
public class TestMutateSpec implements AlgorithmSpec<TestAlgorithm, TestAlgorithmResult, TestMutateConfig, Stream<TestResult>, GraphAlgorithmFactory<TestAlgorithm, TestMutateConfig>> {
    @Override
    public String name() {
        return "TestMutateSpec";
    }

    @Override
    public GraphAlgorithmFactory<TestAlgorithm, TestMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new GraphAlgorithmFactory<>() {

            @Override
            public String taskName() {
                return "TestAlgorithm";
            }

            @Override
            public TestAlgorithm build(
                Graph graph,
                TestMutateConfig configuration,
                ProgressTracker progressTracker
            ) {
                return new TestAlgorithm(
                    graph,
                    progressTracker,
                    configuration.throwInCompute()
                );
            }

            @Override
            public MemoryEstimation memoryEstimation(TestMutateConfig configuration) {
                if (configuration.throwOnEstimate()) {
                    throw new MemoryEstimationNotImplementedException();
                } else {
                    return MemoryEstimations.of("Accurate estimation", MemoryRange.of(42));
                }
            }
        };

    }

    @Override
    public NewConfigFunction<TestMutateConfig> newConfigFunction() {
        return (__, userInput) -> TestMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<TestAlgorithm, TestAlgorithmResult, TestMutateConfig, Stream<TestResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            this::nodePropertyList,
            this::resultBuilder
        );
    }

    private List<NodeProperty> nodePropertyList(ComputationResult<TestAlgorithm, TestAlgorithmResult, TestMutateConfig> computationResult) {
        return List.of(NodeProperty.of(
            computationResult.config().mutateProperty(),
            new LongNodePropertyValues() {
                @Override
                public long longValue(long nodeId) {
                    return nodeId;
                }

                @Override
                public long nodeCount() {
                    return 0;
                }
            }
        ));
    }

    private TestResult.TestResultBuilder resultBuilder(
        ComputationResult<TestAlgorithm, TestAlgorithmResult, TestMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new TestResult.TestResultBuilder().withRelationshipCount(computeResult.result()
            .map(TestAlgorithmResult::relationshipCount)
            .orElse(-1L));
    }
}
