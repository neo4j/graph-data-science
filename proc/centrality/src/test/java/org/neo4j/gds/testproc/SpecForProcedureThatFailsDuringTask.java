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
package org.neo4j.gds.testproc;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.test.config.DummyConfig;
import org.neo4j.gds.test.config.DummyConfigImpl;

import java.util.stream.Stream;

public class SpecForProcedureThatFailsDuringTask implements
    AlgorithmSpec<FailingAlgorithm, OutputFromProcedureThatFailsDuringTask, DummyConfig, Stream<OutputFromProcedureThatFailsDuringTask>, GraphAlgorithmFactory<FailingAlgorithm, DummyConfig>> {
    @Override
    public String name() {
        return "Failing Algorithm";
    }

    @Override
    public GraphAlgorithmFactory<FailingAlgorithm, DummyConfig> algorithmFactory(ExecutionContext executionContext) {
        return new GraphAlgorithmFactory<>() {
            @Override
            public String taskName() {
                return "Failing Algorithm";
            }

            @Override
            public FailingAlgorithm build(
                Graph graph,
                DummyConfig configuration,
                ProgressTracker progressTracker
            ) {
                return new FailingAlgorithm(progressTracker);
            }
        };
    }

    @Override
    public NewConfigFunction<DummyConfig> newConfigFunction() {
        return (username, config) -> new DummyConfigImpl(config);
    }

    @Override
    public ComputationResultConsumer<FailingAlgorithm, OutputFromProcedureThatFailsDuringTask, DummyConfig, Stream<OutputFromProcedureThatFailsDuringTask>> computationResultConsumer() {
        throw new IllegalStateException("We never get this far");
    }
}
