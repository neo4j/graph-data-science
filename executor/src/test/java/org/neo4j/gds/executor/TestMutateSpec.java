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
package org.neo4j.gds.executor;

import org.neo4j.gds.test.TestAlgorithm;
import org.neo4j.gds.test.TestAlgorithmFactory;
import org.neo4j.gds.test.TestAlgorithmResult;
import org.neo4j.gds.test.TestMutateConfig;
import org.neo4j.gds.test.TestMutateConfigImpl;
import org.neo4j.gds.test.TestResult;

public class TestMutateSpec implements AlgorithmSpec<TestAlgorithm, TestAlgorithmResult, TestMutateConfig, TestResult, TestAlgorithmFactory<TestMutateConfig>> {

    @Override
    public String name() {
        return "TestMutate";
    }

    @Override
    public TestAlgorithmFactory<TestMutateConfig> algorithmFactory() {
        return new TestAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<TestMutateConfig> newConfigFunction() {
        return (username, config) -> new TestMutateConfigImpl(config);
    }

    @Override
    public ComputationResultConsumer<TestAlgorithm, TestAlgorithmResult, TestMutateConfig, TestResult> computationResultConsumer() {
        return (computationResult, executionContext) -> new TestResult.TestResultBuilder()
            .withRelationshipCount(computationResult.result().map(TestAlgorithmResult::relationshipCount).orElse(-1L))
            .withNodeCount(computationResult.graph().nodeCount())
            .withPreProcessingMillis(42)
            .withComputeMillis(42)
            .withMutateMillis(42)
            .withWriteMillis(42)
            .withNodePropertiesWritten(42)
            .withConfig(computationResult.config())
            .build();
    }
}
