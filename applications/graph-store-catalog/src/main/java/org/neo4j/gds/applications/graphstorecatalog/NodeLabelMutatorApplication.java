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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.filter.NodesFilter;
import org.neo4j.gds.beta.filter.expression.Expression;
import org.neo4j.gds.config.MutateLabelConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class NodeLabelMutatorApplication {
    public MutateLabelResult compute(
        GraphStore graphStore,
        GraphName graphName,
        String nodeLabelAsString,
        MutateLabelConfig configuration,
        Expression nodeFilter
    ) {
        var resultBuilder = MutateLabelResult.builder(graphName.getValue(), nodeLabelAsString)
            .withConfig(configuration.toMap());

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            var filteredNodes = NodesFilter.filterNodes(
                graphStore,
                nodeFilter,
                configuration.concurrency(),
                Map.of(),
                Pools.DEFAULT,
                ProgressTracker.NULL_TRACKER
            );

            var nodeCounter = new LongAdder();
            var idMap = filteredNodes.idMap();
            var nodeLabelToMutate = NodeLabel.of(nodeLabelAsString);
            graphStore.addNodeLabel(nodeLabelToMutate);
            idMap.forEachNode(
                nodeId -> {
                    var originalNodeId = idMap.toOriginalNodeId(nodeId);
                    var mappedNodeId = graphStore.nodes().safeToMappedNodeId(originalNodeId);
                    graphStore.nodes().addNodeIdToLabel(mappedNodeId, nodeLabelToMutate);
                    nodeCounter.increment();
                    return true;
                }
            );

            resultBuilder
                .withNodeLabelsWritten(nodeCounter.longValue())
                .withNodeCount(graphStore.nodeCount());
        }

        return resultBuilder.build();
    }
}
