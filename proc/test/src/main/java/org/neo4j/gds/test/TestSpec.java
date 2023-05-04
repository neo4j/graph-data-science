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

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.test.Constants.STATS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.testProc.write", description = STATS_DESCRIPTION, executionMode = STATS)
public class TestSpec implements AlgorithmSpec<TestAlgorithm, TestAlgorithmResult, TestWriteConfig, Stream<TestResult>, TestAlgorithmFactory<TestWriteConfig>> {
    @Override
    public String name() {
        return "TestSpec";
    }

    @Override
    public TestAlgorithmFactory<TestWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new TestAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<TestWriteConfig> newConfigFunction() {
        return (__, userInput) -> TestWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<TestAlgorithm, TestAlgorithmResult, TestWriteConfig, Stream<TestResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging("Stats call failed",
            executionContext.log(),
            () -> Stream.of(
            resultBuilder(computationResult, executionContext)
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(computationResult.config())
                .build()
        ));
    }

    private TestResult.TestResultBuilder resultBuilder(
        ComputationResult<TestAlgorithm, TestAlgorithmResult, TestWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new TestResult.TestResultBuilder().withRelationshipCount(computeResult.result()
            .map(TestAlgorithmResult::relationshipCount)
            .orElse(-1L));
    }
}
