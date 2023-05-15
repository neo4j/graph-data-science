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
import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodePropertyListFunction;
import org.neo4j.gds.ResultBuilderFunction;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.config.MutatePropertyConfig;
import org.neo4j.gds.core.huge.FilteredNodePropertyValues;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

public abstract class PregelMutateComputationResultConsumer<
    ALGO extends Algorithm<PregelResult>,
    CONFIG extends PregelProcedureConfig
  > extends
    MutateComputationResultConsumer<ALGO, PregelResult, CONFIG, PregelMutateResult> {

    private final MutatePropertyComputationResultConsumer.MutateNodePropertyListFunction<ALGO, PregelResult, CONFIG> nodePropertyListFunction;

    public PregelMutateComputationResultConsumer() {
        super((computationResult, executionContext) -> {
            var ranIterations = computationResult.result().map(PregelResult::ranIterations).orElse(0);
            var didConverge = computationResult.result().map(PregelResult::didConverge).orElse(false);
            return new PregelMutateResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
        });
        this.nodePropertyListFunction = (computationResult) -> PregelBaseProc.nodeProperties(
            computationResult,
            computationResult.config().mutateProperty()
        );
    }

    @Override
    protected void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, PregelResult, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        var graph = computationResult.graph();
        MutatePropertyConfig mutatePropertyConfig = computationResult.config();

        final var nodeProperties = this.nodePropertyListFunction.apply(computationResult);

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
