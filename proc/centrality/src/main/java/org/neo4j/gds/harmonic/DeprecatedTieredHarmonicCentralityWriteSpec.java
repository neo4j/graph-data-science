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
package org.neo4j.gds.harmonic;

import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.EmptyDoubleNodePropertyValues;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.centrality.alphaharmonic.AlphaHarmonicWriteResult;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.harmonic.HarmonicCentralityCompanion.DESCRIPTION;

@GdsCallable(name = "gds.alpha.closeness.harmonic.write", description = DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class DeprecatedTieredHarmonicCentralityWriteSpec implements AlgorithmSpec<HarmonicCentrality, HarmonicResult, DeprecatedTieredHarmonicCentralityWriteConfig, Stream<AlphaHarmonicWriteResult>, HarmonicCentralityAlgorithmFactory<DeprecatedTieredHarmonicCentralityWriteConfig>> {

    @Override
    public String name() {
        return "HarmonicCentralityWrite";
    }

    @Override
    public HarmonicCentralityAlgorithmFactory<DeprecatedTieredHarmonicCentralityWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new HarmonicCentralityAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<DeprecatedTieredHarmonicCentralityWriteConfig> newConfigFunction() {
        return (___,config) -> DeprecatedTieredHarmonicCentralityWriteConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<HarmonicCentrality, HarmonicResult, DeprecatedTieredHarmonicCentralityWriteConfig, Stream<AlphaHarmonicWriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().writeProperty(),
                computationResult.result()
                    .map(HarmonicResult::nodePropertyValues)
                    .orElse(EmptyDoubleNodePropertyValues.INSTANCE)
            )),
            name()
        );
    }

    private AbstractCentralityResultBuilder<AlphaHarmonicWriteResult> resultBuilder(
        ComputationResult<HarmonicCentrality, HarmonicResult, DeprecatedTieredHarmonicCentralityWriteConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new AlphaHarmonicWriteResult.Builder(
            executionContext.returnColumns(),
            computationResult.config().concurrency()
        ).withWriteProperty(computationResult.config().writeProperty());

        computationResult.result()
            .ifPresent(result -> builder.withCentralityFunction(result.centralityScoreProvider()));


        return builder;

    }
}
