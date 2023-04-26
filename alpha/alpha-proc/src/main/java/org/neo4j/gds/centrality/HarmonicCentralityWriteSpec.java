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
package org.neo4j.gds.centrality;

import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.impl.harmonic.HarmonicCentrality;
import org.neo4j.gds.impl.harmonic.HarmonicCentralityAlgorithmFactory;
import org.neo4j.gds.impl.harmonic.HarmonicCentralityWriteConfig;
import org.neo4j.gds.impl.harmonic.HarmonicResult;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.centrality.HarmonicCentralityProc.DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;

@GdsCallable(name = "gds.alpha.closeness.harmonic.write", description = DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class HarmonicCentralityWriteSpec implements AlgorithmSpec<HarmonicCentrality, HarmonicResult, HarmonicCentralityWriteConfig,Stream<WriteResult>, HarmonicCentralityAlgorithmFactory<HarmonicCentralityWriteConfig>> {

    @Override
    public String name() {
        return "HarmonicCentralityWrite";
    }

    @Override
    public HarmonicCentralityAlgorithmFactory<HarmonicCentralityWriteConfig> algorithmFactory() {
        return new HarmonicCentralityAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<HarmonicCentralityWriteConfig> newConfigFunction() {
        return (___,config) -> HarmonicCentralityWriteConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<HarmonicCentrality, HarmonicResult, HarmonicCentralityWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            computationResult -> {
                var result=computationResult.result().get();
                var nodeProperties=new DoubleNodePropertyValues(){
                   @Override
                   public double doubleValue(long nodeId) {
                       return result.getCentralityScore(nodeId);
                   }
                   @Override
                   public long nodeCount() {
                       return computationResult.graph().nodeCount();
                   }
               };
              return   List.of(ImmutableNodeProperty.of(
                    computationResult.config().writeProperty(),
                    nodeProperties
                ));
            },
            name()
        );
    }

    private AbstractCentralityResultBuilder<WriteResult> resultBuilder(
        ComputationResult<HarmonicCentrality, HarmonicResult, HarmonicCentralityWriteConfig> computationResult,
        ExecutionContext executionContext
    ) {

        var builder=new WriteResult.Builder(executionContext.returnColumns(),computationResult.config().concurrency())
            .withWriteProperty(computationResult.config().writeProperty());

        computationResult.result().ifPresent(result -> builder.withCentralityFunction( nodeId-> result.getCentralityScore(nodeId)));

        return builder;

    }
}
