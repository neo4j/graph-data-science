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
package org.neo4j.gds.wcc;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.NewConfigFunction;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.MutatePropertyConfig;
import org.neo4j.gds.core.huge.FilteredNodeProperties;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.pipeline.AlgorithmSpec;
import org.neo4j.gds.pipeline.ComputationResultConsumer;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WccMutateSpec implements AlgorithmSpec<Wcc, DisjointSetStruct, WccMutateConfig, Stream<WccMutateProc.MutateResult>> {

    private final ProcedureCallContext callContext;
    private final AllocationTracker allocationTracker;
    private final Log log;

    public WccMutateSpec(
        ProcedureCallContext callContext,
        AllocationTracker allocationTracker,
        Log log
    ) {
        this.callContext = callContext;
        this.allocationTracker = allocationTracker;
        this.log = log;
    }

    @Override
    public AlgorithmFactory<Wcc, WccMutateConfig> algorithmFactory() {
        return new WccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<WccMutateConfig> newConfigFunction() {
        return (__, graphName, maybeImplicitCreate, config) -> WccMutateConfig.of(graphName, maybeImplicitCreate, config);
    }

    @Override
    public ComputationResultConsumer<Wcc, DisjointSetStruct, WccMutateConfig, Stream<WccMutateProc.MutateResult>> computationResultConsumer() {
        return computationResult -> {
            var resultBuilder = WccProc.resultBuilder(
                new WccMutateProc.MutateResult.Builder(
                    callContext,
                    computationResult.config().concurrency(),
                    allocationTracker
                ),
                computationResult
            );
            var config = computationResult.config();

            AbstractResultBuilder<WccMutateProc.MutateResult> builder = resultBuilder
                .withCreateMillis(computationResult.createMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(config);

            if (computationResult.isGraphEmpty()) {
                return Stream.of(builder.build());
            } else {
                updateGraphStore(builder, computationResult);
                computationResult.graph().releaseProperties();
                return Stream.of(builder.build());
            }
        };
    }

    private void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        AlgoBaseProc.ComputationResult<Wcc, DisjointSetStruct, WccMutateConfig> computationResult
    ) {
        Graph graph = computationResult.graph();

        var nodeProperties = List.of(ImmutableNodeProperty.of(
            computationResult.config().mutateProperty(),
            WccProc.nodeProperties(computationResult, computationResult.config().mutateProperty(), allocationTracker)
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

            resultBuilder.withNodePropertiesWritten(nodeProperties.size() * computationResult.graph().nodeCount());
        }
    }
}
