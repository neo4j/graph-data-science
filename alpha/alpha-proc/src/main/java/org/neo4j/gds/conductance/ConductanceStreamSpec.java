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
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.impl.conductance.Conductance;
import org.neo4j.gds.impl.conductance.ConductanceFactory;
import org.neo4j.gds.impl.conductance.ConductanceResult;
import org.neo4j.gds.impl.conductance.ConductanceStreamConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.impl.conductance.Conductance.CONDUCTANCE_DESCRIPTION;

@GdsCallable(name = "gds.alpha.conductance.stream", description = CONDUCTANCE_DESCRIPTION, executionMode = STREAM)
public class ConductanceStreamSpec implements AlgorithmSpec<Conductance, ConductanceResult, ConductanceStreamConfig, Stream<StreamResult>, ConductanceFactory<ConductanceStreamConfig>> {

    @Override
    public String name() {
        return "ConductanceStream";
    }

    @Override
    public ConductanceFactory<ConductanceStreamConfig> algorithmFactory() {
        return new ConductanceFactory<>();
    }

    @Override
    public NewConfigFunction<ConductanceStreamConfig> newConfigFunction() {
        return (username, configuration) -> ConductanceStreamConfig.of(configuration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ComputationResultConsumer<Conductance, ConductanceResult, ConductanceStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.isGraphEmpty()) {
                return Stream.empty();
            }
            var result=computationResult.result();
            var condunctances = result.communityConductances();

            return LongStream
                .range(0, condunctances.capacity())
                .filter(c -> !Double.isNaN(condunctances.get(c)))
                .mapToObj(c ->  new StreamResult(c, condunctances.get(c)));
        };

    }

}
