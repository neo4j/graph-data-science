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
package org.neo4j.gds.labelpropagation;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.labelpropagation.LabelPropagation.LABEL_PROPAGATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class LabelPropagationMutateProc extends MutatePropertyProc<LabelPropagation, LabelPropagationResult, MutateResult, LabelPropagationMutateConfig> {

    @Procedure(value = "gds.labelPropagation.mutate", mode = READ)
    @Description(LABEL_PROPAGATION_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return new ProcedureExecutor<>(
            new LabelPropagationMutateSpecification(),
            executionContext()
        ).compute(graphName, configuration);
    }

    @Procedure(value = "gds.labelPropagation.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return new MemoryEstimationExecutor<>(
            new LabelPropagationMutateSpecification(),
            executionContext(),
            transactionContext()
        ).computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected LabelPropagationMutateConfig newConfig(String username, CypherMapWrapper config) {
        return LabelPropagationMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<LabelPropagation, LabelPropagationMutateConfig> algorithmFactory() {
        return new LabelPropagationFactory<>();
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<LabelPropagation, LabelPropagationResult, LabelPropagationMutateConfig> computationResult) {
        return CommunityProcCompanion.nodeProperties(
            computationResult.config(),
            computationResult.config().mutateProperty(),
            computationResult.result()
                .map(LabelPropagationResult::labels)
                .orElseGet(() -> HugeLongArray.newArray(0))
                .asNodeProperties(),
            () -> computationResult.graphStore().nodeProperty(computationResult.config().seedProperty())
        );
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<LabelPropagation, LabelPropagationResult, LabelPropagationMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        return new LabelPropagationMutateSpecification().resultBuilder(computationResult, executionContext);
    }

}
