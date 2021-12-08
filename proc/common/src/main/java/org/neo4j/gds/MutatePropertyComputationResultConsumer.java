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
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.MutatePropertyConfig;
import org.neo4j.gds.core.huge.FilteredNodeProperties;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.logging.Log;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class MutatePropertyComputationResultConsumer<ALGO extends Algorithm<ALGO, ALGO_RESULT>, ALGO_RESULT, CONFIG extends MutatePropertyConfig, RESULT>
    extends MutateComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, RESULT> {

    public interface NodePropertiesFunction<ALGO extends Algorithm<ALGO, ALGO_RESULT>, ALGO_RESULT, CONFIG extends MutatePropertyConfig> {
        NodeProperties apply(
            AlgoBaseProc.ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult,
            String resultProperty,
            AllocationTracker allocationTracker
        );
    }

    private final NodePropertiesFunction<ALGO, ALGO_RESULT, CONFIG> nodePropertiesFunction;
    private final AllocationTracker allocationTracker;

    public MutatePropertyComputationResultConsumer(
        NodePropertiesFunction<ALGO, ALGO_RESULT, CONFIG> nodePropertiesFunction,
        ResultBuilderFunction<ALGO, ALGO_RESULT, CONFIG, RESULT> resultBuilderFunction,
        Log log,
        AllocationTracker allocationTracker
    ) {
        super(resultBuilderFunction, log);
        this.nodePropertiesFunction = nodePropertiesFunction;
        this.allocationTracker = allocationTracker;
    }

    @Override
    protected void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        AlgoBaseProc.ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        var graph = computationResult.graph();
        MutatePropertyConfig mutatePropertyConfig = computationResult.config();

        var nodeProperties = List.of(ImmutableNodeProperty.of(
            computationResult.config().mutateProperty(),
            this.nodePropertiesFunction.apply(computationResult, mutatePropertyConfig.mutateProperty(), allocationTracker)
        ));

        if (graph instanceof NodeFilteredGraph) {
            nodeProperties = nodeProperties.stream().map(nodeProperty ->
                    ImmutableNodeProperty.of(
                        nodeProperty.propertyKey(),
                        new FilteredNodeProperties.OriginalToFilteredNodeProperties(
                            nodeProperty.properties(),
                            (NodeFilteredGraph) graph
                        )
                    )
                )
                .collect(Collectors.toList());
        }

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            log.debug("Updating in-memory graph store");
            GraphStore graphStore = computationResult.graphStore();
            Collection<NodeLabel> labelsToUpdate = mutatePropertyConfig.nodeLabelIdentifiers(graphStore);

            nodeProperties.forEach(nodeProperty -> {
                for (NodeLabel label : labelsToUpdate) {
                    graphStore.addNodeProperty(
                        label,
                        nodeProperty.propertyKey(),
                        nodeProperty.properties()
                    );
                }
            });

            resultBuilder.withNodePropertiesWritten(nodeProperties.size() * computationResult.graph().nodeCount());
        }
    }
}
