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
package org.neo4j.gds.conductance;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.community.conductance.ConductanceStreamResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.conductance.Conductance.CONDUCTANCE_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.conductance.stream", aliases = {"gds.alpha.conductance.stream"}, description = CONDUCTANCE_DESCRIPTION, executionMode = STREAM)
public class ConductanceStreamSpec implements AlgorithmSpec<Conductance, ConductanceResult, ConductanceStreamConfig, Stream<ConductanceStreamResult>, ConductanceAlgorithmFactory<ConductanceStreamConfig>> {

    @Override
    public String name() {
        return "ConductanceStream";
    }

    @Override
    public ConductanceAlgorithmFactory<ConductanceStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ConductanceAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<ConductanceStreamConfig> newConfigFunction() {
        return (username, configuration) -> ConductanceStreamConfig.of(configuration);
    }

    @Override
    public ComputationResultConsumer<Conductance, ConductanceResult, ConductanceStreamConfig, Stream<ConductanceStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var condunctances = result.communityConductances();
                    return LongStream
                        .range(0, condunctances.capacity())
                        .filter(community -> !Double.isNaN(condunctances.get(community)))
                        .mapToObj(community -> new ConductanceStreamResult(community, condunctances.get(community)));
                }).orElseGet(Stream::empty)
        );
    }
}
