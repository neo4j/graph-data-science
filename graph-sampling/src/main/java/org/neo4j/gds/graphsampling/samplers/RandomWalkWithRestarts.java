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
package org.neo4j.gds.graphsampling.samplers;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.beta.filter.GraphStoreFilter;
import org.neo4j.gds.beta.filter.ImmutableFilteredNodes;
import org.neo4j.gds.beta.filter.NodesFilter;
import org.neo4j.gds.beta.filter.RelationshipsFilter;
import org.neo4j.gds.beta.filter.expression.EvaluationContext;
import org.neo4j.gds.beta.filter.expression.Expression;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

public class RandomWalkWithRestarts {
    private final RandomWalkWithRestartsConfig config;
    private final GraphStore inputGraphStore;

    public RandomWalkWithRestarts(RandomWalkWithRestartsConfig config, GraphStore inputGraphStore) {
        this.config = config;
        this.inputGraphStore = inputGraphStore;
    }

    public GraphStore sample() {
        var inputGraph = inputGraphStore.getGraph(
            config.nodeLabelIdentifiers(inputGraphStore),
            config.internalRelationshipTypes(inputGraphStore),
            Optional.empty()
        );
        var rng = new SplittableRandom(config.randomSeed().orElseGet(() -> new SplittableRandom().nextLong()));

        IdMap sampledNodes = sampleNodes(inputGraph, rng);

        var nodePropertyStore = NodesFilter.filterNodeProperties(
            inputGraphStore,
            sampledNodes,
            config.concurrency(),
            ProgressTracker.NULL_TRACKER
        );

        var relTypeFilterExpression = new Expression() {
            private final List<String> types = config
                .internalRelationshipTypes(inputGraphStore)
                .stream()
                .map(RelationshipType::name)
                .collect(Collectors.toList());

            @Override
            public double evaluate(EvaluationContext context) {
                return context.hasLabelsOrTypes(types) ? Expression.TRUE : Expression.FALSE;
            }
        };
        var filteredRelationships = RelationshipsFilter.filterRelationships(
            inputGraphStore,
            relTypeFilterExpression,
            inputGraphStore.nodes(),
            sampledNodes,
            config.concurrency(),
            Map.of(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var filteredSchema = GraphStoreFilter.filterSchema(
            inputGraphStore.schema(),
            ImmutableFilteredNodes.of(sampledNodes, nodePropertyStore),
            filteredRelationships
        );

        return new GraphStoreBuilder()
            .databaseId(inputGraphStore.databaseId())
            .capabilities(inputGraphStore.capabilities())
            .schema(filteredSchema)
            .nodes(sampledNodes)
            .nodePropertyStore(nodePropertyStore)
            .relationships(filteredRelationships.topology())
            .relationshipPropertyStores(filteredRelationships.propertyStores())
            .concurrency(config.concurrency())
            .build();
    }

    private IdMap sampleNodes(Graph inputGraph, SplittableRandom rng) {
        boolean hasLabelInformation = !inputGraphStore.nodeLabels().isEmpty();
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(config.concurrency())
            .maxOriginalId(inputGraph.highestNeoId())
            .hasProperties(false)
            .hasLabelInformation(hasLabelInformation)
            .deduplicateIds(false)
            .build();

        long expectedNodes = Math.round(inputGraph.nodeCount() * config.samplingRatio());
        final long startNode = config.startNode().map(inputGraph::toMappedNodeId).orElse(0L);
        var currentNode = new MutableLong(startNode);
        // must keep track of this because nodesBuilder may not have flushed its buffer, so importedNodes cannot be used atm
        var seen = HugeAtomicBitSet.create(inputGraph.nodeCount());
        while (seen.cardinality() < expectedNodes) {
            if (!seen.get(currentNode.getValue())) {
                long originalId = inputGraph.toOriginalNodeId(currentNode.getValue());
                if (hasLabelInformation) {
                    var nodeLabelList = inputGraph.nodeLabels(currentNode.getValue());
                    var nodeLabels = new NodeLabel[nodeLabelList.size()];
                    nodeLabelList.toArray(nodeLabels);
                    nodesBuilder.addNode(originalId, nodeLabels);
                } else {
                    nodesBuilder.addNode(originalId);
                }
                seen.set(currentNode.getValue());
            }
            currentNode.setValue(walkStep(currentNode, startNode, inputGraph, rng));
        }
        var idMapAndProperties = nodesBuilder.build();

        return idMapAndProperties.idMap();
    }

    private long walkStep(MutableLong currentNode, long startNode, Graph inputGraph, SplittableRandom rng) {
        int degree = inputGraph.degree(currentNode.getValue());
        if (degree == 0 || rng.nextDouble() < config.restartProbability()) {
            return startNode;
        }
        int targetOffset = rng.nextInt(degree);

        return inputGraph.getNeighbor(currentNode.getValue(), targetOffset);
    }
}
