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
package org.neo4j.gds;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.MutatePropertyConfig;
import org.neo4j.gds.core.huge.FilteredNodePropertyValues;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

public class MutatePropertyComputationResultConsumer<ALGO extends Algorithm<ALGO_RESULT>, ALGO_RESULT, CONFIG extends MutatePropertyConfig, RESULT>
    extends MutateComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, RESULT> {

    public interface MutateNodePropertyListFunction<ALGO extends Algorithm<ALGO_RESULT>, ALGO_RESULT, CONFIG extends MutatePropertyConfig>
        extends NodePropertyListFunction<ALGO, ALGO_RESULT, CONFIG> {}

    private final MutateNodePropertyListFunction<ALGO, ALGO_RESULT, CONFIG> nodePropertyListFunction;

    public MutatePropertyComputationResultConsumer(
        MutateNodePropertyListFunction<ALGO, ALGO_RESULT, CONFIG> nodePropertyListFunction,
        ResultBuilderFunction<ALGO, ALGO_RESULT, CONFIG, RESULT> resultBuilderFunction
    ) {
        super(resultBuilderFunction);
        this.nodePropertyListFunction = nodePropertyListFunction;
    }

    @Override
    protected void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        var graph = computationResult.graph();
        MutatePropertyConfig mutatePropertyConfig = computationResult.config();

        final var nodeProperties = this.nodePropertyListFunction.apply(computationResult);

        var maybeTranslatedProperties = graph.asNodeFilteredGraph()
            .map(filteredGraph -> nodeProperties.stream()
                .map(nodeProperty ->
                    ImmutableNodeProperty.of(
                        nodeProperty.propertyKey(),
                        new FilteredNodePropertyValues.OriginalToFilteredNodePropertyValues(
                            nodeProperty.properties(),
                            filteredGraph
                        )
                    )
                ).collect(Collectors.toList())
            ).orElse(nodeProperties);

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            executionContext.log().debug("Updating in-memory graph store");
            GraphStore graphStore = computationResult.graphStore();
            Collection<NodeLabel> labelsToUpdate = mutatePropertyConfig.nodeLabelIdentifiers(graphStore);

            maybeTranslatedProperties.forEach(nodeProperty -> graphStore.addNodeProperty(
                new HashSet<>(labelsToUpdate),
                nodeProperty.propertyKey(),
                nodeProperty.properties()
            ));

            resultBuilder.withNodePropertiesWritten(maybeTranslatedProperties.size() * computationResult.graph().nodeCount());
        }
    }
}
