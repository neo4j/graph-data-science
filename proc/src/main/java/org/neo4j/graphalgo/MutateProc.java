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
import org.neo4j.graphalgo.config.MutatePropertyConfig;
import org.neo4j.graphalgo.core.huge.NodeFilteredGraph;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;

import java.util.stream.Stream;

public abstract class MutateProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    PROC_RESULT,
    CONFIG extends MutatePropertyConfig> extends AlgoBaseProc<ALGO, ALGO_RESULT, CONFIG> {

    protected abstract PropertyTranslator<ALGO_RESULT> nodePropertyTranslator(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult);

    protected abstract AbstractResultBuilder<PROC_RESULT> resultBuilder(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult);

    protected Stream<PROC_RESULT> mutate(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult) {
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
    }

    private void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        PropertyTranslator<ALGO_RESULT> resultPropertyTranslator = nodePropertyTranslator(computationResult);
        MutatePropertyConfig mutatePropertyConfig = computationResult.config();
        ALGO_RESULT result = computationResult.result();
        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            log.debug("Updating in-memory graph store");
            GraphStore graphStore = computationResult.graphStore();
            Graph graph = computationResult.graph();
            if (graph instanceof NodeFilteredGraph) {
                NodeFilteredGraph filteredGraph = (NodeFilteredGraph) graph;
                graphStore.addNodeProperty(
                    mutatePropertyConfig.mutateProperty(),
                    nodeId -> !graph.contains(nodeId) ? Double.NaN : resultPropertyTranslator.toDouble(result, filteredGraph.getMappedNodeId(nodeId))
                );
            }
            else {
                graphStore.addNodeProperty(
                    mutatePropertyConfig.mutateProperty(),
                    nodeId -> resultPropertyTranslator.toDouble(result, nodeId)
                );
            }
            resultBuilder.withNodePropertiesWritten(computationResult.graph().nodeCount());
        }
    }
}
