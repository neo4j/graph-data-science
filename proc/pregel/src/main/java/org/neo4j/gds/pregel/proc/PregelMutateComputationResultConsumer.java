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
package org.neo4j.gds.pregel.proc;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.config.MutatePropertyConfig;
import org.neo4j.gds.core.huge.FilteredNodePropertyValues;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

public abstract class PregelMutateComputationResultConsumer<
    ALGO extends Algorithm<PregelResult>,
    CONFIG extends PregelProcedureConfig
  > implements ComputationResultConsumer<ALGO, PregelResult, CONFIG, Stream<PregelMutateResult>> {

    @Override
    public Stream<PregelMutateResult> consume(ComputationResult<ALGO, PregelResult, CONFIG> computationResult, ExecutionContext executionContext) {
        return runWithExceptionLogging("Graph mutation failed", executionContext.log(), () -> {
            CONFIG config = computationResult.config();

            var ranIterations = computationResult.result().map(PregelResult::ranIterations).orElse(0);
            var didConverge = computationResult.result().map(PregelResult::didConverge).orElse(false);
            AbstractResultBuilder<PregelMutateResult> builder = new PregelMutateResult.Builder()
                .withRanIterations(ranIterations)
                .didConverge(didConverge)
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(config);

            try (ProgressTimer ignored = ProgressTimer.start(builder::withMutateMillis)) {
                if (!computationResult.isGraphEmpty()) {
                    updateGraphStore(builder, computationResult, executionContext);
                }
            }

            return Stream.of(builder.build());
        });
    }

    private void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, PregelResult, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        var graph = computationResult.graph();
        MutatePropertyConfig mutatePropertyConfig = computationResult.config();

        final var nodeProperties = PregelBaseProc.nodeProperties(
            computationResult,
            computationResult.config().mutateProperty()
        );

        var maybeTranslatedProperties = graph
            .asNodeFilteredGraph()
            .map(filteredGraph -> nodeProperties
                .stream()
                .map(nodeProperty -> ImmutableNodeProperty.of(nodeProperty.propertyKey(),
                    new FilteredNodePropertyValues.OriginalToFilteredNodePropertyValues(nodeProperty.properties(),
                        filteredGraph
                    )
                ))
                .collect(Collectors.toList()))
            .orElse(nodeProperties);

        executionContext.log().debug("Updating in-memory graph store");
        GraphStore graphStore = computationResult.graphStore();
        Collection<NodeLabel> labelsToUpdate = mutatePropertyConfig.nodeLabelIdentifiers(graphStore);

        maybeTranslatedProperties.forEach(nodeProperty -> graphStore.addNodeProperty(new HashSet<>(labelsToUpdate),
            nodeProperty.propertyKey(),
            nodeProperty.properties()
        ));

        resultBuilder.withNodePropertiesWritten(maybeTranslatedProperties.size() * computationResult
            .graph()
            .nodeCount());
    }
}
