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
package org.neo4j.gds.traverse;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.impl.traverse.Traverse;
import org.neo4j.gds.impl.traverse.TraverseConfig;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.InputNodeValidator.validateEndNode;
import static org.neo4j.gds.utils.InputNodeValidator.validateStartNode;

public class TraverseFactory<CONFIG extends TraverseConfig> extends GraphAlgorithmFactory<Traverse, CONFIG> {

    public TraverseFactory() {
        super();
    }

    @Override
    public Traverse build(
        Graph graphOrGraphStore,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        Traverse.ExitPredicate exitFunction;
        Traverse.Aggregator aggregatorFunction;
        // target node given; terminate if target is reached
        if (!configuration.targetNodes().isEmpty()) {
            List<Long> mappedTargets = configuration.targetNodes().stream()
                .map(graphOrGraphStore::safeToMappedNodeId)
                .collect(Collectors.toList());
            exitFunction = (s, t, w) -> mappedTargets.contains(t) ? Traverse.ExitPredicate.Result.BREAK : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
            // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (configuration.maxDepth() != -1) {
            exitFunction = (s, t, w) -> w > configuration.maxDepth() ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + 1.;
            // do complete BFS until all nodes have been visited
        } else {
            exitFunction = (s, t, w) -> Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        }

        validateStartNode(configuration.startNode(), graphOrGraphStore);
        configuration.targetNodes().forEach(neoId -> validateEndNode(neoId, graphOrGraphStore));

        var mappedStartNodeId = graphOrGraphStore.toMappedNodeId(configuration.startNode());

        return configuration.isBfs()
            ? Traverse.bfs(
            graphOrGraphStore,
            mappedStartNodeId,
            exitFunction,
            aggregatorFunction,
            progressTracker
        )
            : Traverse.dfs(
                graphOrGraphStore,
                mappedStartNodeId,
                exitFunction,
                aggregatorFunction,
                progressTracker
            );
    }

    @Override
    public String taskName() {
        return "Traverse";
    }


    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf(taskName(), graph.relationshipCount());
    }
    
    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(Traverse.class);
        builder.perNode("visited ", MemoryUsage::sizeOfBitset);
        builder.perNode("nodes", MemoryUsage::sizeOfLongArrayList);
        builder.perNode("sources", MemoryUsage::sizeOfLongArrayList);
        builder.perNode("weights", MemoryUsage::sizeOfDoubleArrayList);
        builder.perNode("resultNodes", MemoryUsage::sizeOfLongArray);


        return builder.build();
    }
}
