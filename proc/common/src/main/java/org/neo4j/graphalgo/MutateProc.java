/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.MutatePropertyConfig;
import org.neo4j.graphalgo.core.huge.FilteredNodeProperties;
import org.neo4j.graphalgo.core.huge.NodeFilteredGraph;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.ImmutableNodeProperty;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.result.AbstractResultBuilder;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class MutateProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    PROC_RESULT,
    CONFIG extends MutatePropertyConfig> extends AlgoBaseProc<ALGO, ALGO_RESULT, CONFIG> {

    protected abstract NodeProperties getNodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult);

    protected abstract AbstractResultBuilder<PROC_RESULT> resultBuilder(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult);

    protected Stream<PROC_RESULT> mutate(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult) {
        return runWithExceptionLogging("Graph mutation failed", () -> {
            CONFIG config = computeResult.config();

            AbstractResultBuilder<PROC_RESULT> builder = resultBuilder(computeResult)
                .withCreateMillis(computeResult.createMillis())
                .withComputeMillis(computeResult.computeMillis())
                .withNodeCount(computeResult.graph().nodeCount())
                .withConfig(config);

            if (computeResult.isGraphEmpty()) {
                return Stream.of(builder.build());
            } else {
                updateGraphStore(builder, computeResult);
                computeResult.graph().releaseProperties();
                return Stream.of(builder.build());
            }
        });
    }

    protected List<NodePropertyExporter.NodeProperty<?>> nodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        return List.of(ImmutableNodeProperty.of(computationResult.config().mutateProperty(), getNodeProperties(computationResult)));
    }

    private void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        Graph graph = computationResult.graph();

        var nodeProperties = nodeProperties(computationResult);

        if (graph instanceof NodeFilteredGraph) {
            nodeProperties = nodeProperties.stream().map(nodeProperty ->
                ImmutableNodeProperty.of(
                    nodeProperty.propertyKey(),
                    new ReverseFilteredNodeProperties(nodeProperty.properties(), (NodeFilteredGraph) graph)
                )
            )
            .collect(Collectors.toList());
        }

        MutatePropertyConfig mutatePropertyConfig = computationResult.config();

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

            resultBuilder.withNodePropertiesWritten(computationResult.graph().nodeCount());
        }
    }

    static class ReverseFilteredNodeProperties extends FilteredNodeProperties {
        ReverseFilteredNodeProperties(NodeProperties properties, NodeFilteredGraph idMap) {
            super(properties, idMap);
        }

        @Override
        protected long translateId(long nodeId) {
            return graph.getFilteredMappedNodeId(nodeId);
        }
    }
}
