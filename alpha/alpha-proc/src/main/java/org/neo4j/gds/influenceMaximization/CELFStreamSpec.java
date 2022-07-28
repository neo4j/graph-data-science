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
package org.neo4j.gds.influenceMaximization;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.influenceMaximization.CELFStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.alpha.influenceMaximization.celf.stream", description = DESCRIPTION, executionMode = STREAM)
public class CELFStreamSpec implements AlgorithmSpec<CELF, LongDoubleScatterMap, InfluenceMaximizationConfig, Stream<InfluenceMaximizationResult>, CELFAlgorithmFactory> {

    @Override
    public String name() {
        return "CELFStream";
    }

    @Override
    public CELFAlgorithmFactory algorithmFactory() {
        return new CELFAlgorithmFactory();
    }

    @Override
    public NewConfigFunction<InfluenceMaximizationConfig> newConfigFunction() {
        return (__, userInput) -> InfluenceMaximizationConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<CELF, LongDoubleScatterMap, InfluenceMaximizationConfig, Stream<InfluenceMaximizationResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var celfSeedSetMap = computationResult.result();
            if (celfSeedSetMap == null) {
                return Stream.empty();
            }

            computationResult.graph().release();
            return Optional.ofNullable(computationResult.algorithm())
                .map(CELF::resultStream)
                .orElseGet(Stream::empty);
        };

    }
}
