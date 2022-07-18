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
package org.neo4j.gds.graphsampling;

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
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.graphsampling.samplers.NodesSampler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class GraphSampleConstructor {
    private final AlgoBaseConfig config;
    private final GraphStore inputGraphStore;
    private final NodesSampler nodesSampler;

    public GraphSampleConstructor(AlgoBaseConfig config, GraphStore inputGraphStore, NodesSampler nodesSampler) {
        this.config = config;
        this.inputGraphStore = inputGraphStore;
        this.nodesSampler = nodesSampler;
    }

    public GraphStore construct() {
        var idMap = computeIdMap();

        var nodePropertyStore = NodesFilter.filterNodeProperties(
            inputGraphStore,
            idMap,
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
            idMap,
            config.concurrency(),
            Map.of(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var filteredSchema = GraphStoreFilter.filterSchema(
            inputGraphStore.schema(),
            ImmutableFilteredNodes.of(idMap, nodePropertyStore),
            filteredRelationships
        );

        return new GraphStoreBuilder()
            .databaseId(inputGraphStore.databaseId())
            .capabilities(inputGraphStore.capabilities())
            .schema(filteredSchema)
            .nodes(idMap)
            .nodePropertyStore(nodePropertyStore)
            .relationships(filteredRelationships.topology())
            .relationshipPropertyStores(filteredRelationships.propertyStores())
            .concurrency(config.concurrency())
            .build();
    }

    private IdMap computeIdMap() {
        var inputGraph = inputGraphStore.getGraph(
            config.nodeLabelIdentifiers(inputGraphStore),
            config.internalRelationshipTypes(inputGraphStore),
            Optional.empty()
        );
        var sampledNodesBitSet = nodesSampler.sampleNodes(inputGraph);

        boolean hasLabelInformation = !inputGraphStore.nodeLabels().isEmpty();
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(config.concurrency())
            .maxOriginalId(inputGraph.highestNeoId())
            .hasProperties(false)
            .hasLabelInformation(hasLabelInformation)
            .deduplicateIds(false)
            .build();
        var tasks = PartitionUtils.rangePartition(config.concurrency(),
            inputGraph.nodeCount(),
            partition -> new IdMapSampleTask(
                nodesBuilder,
                sampledNodesBitSet,
                inputGraph,
                hasLabelInformation,
                partition
            ),
            Optional.empty()
        );
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .run();

        return nodesBuilder.build().idMap();
    }

    static class IdMapSampleTask implements Runnable {
        private final NodesBuilder nodesBuilder;
        private final HugeAtomicBitSet nodesBitSet;
        private final Graph inputGraph;
        private final boolean hasLabelInformation;
        private final Partition partition;

        IdMapSampleTask(
            NodesBuilder nodesBuilder,
            HugeAtomicBitSet nodesBitSet,
            Graph inputGraph,
            boolean hasLabelInformation,
            Partition partition
        ) {
            this.nodesBuilder = nodesBuilder;
            this.nodesBitSet = nodesBitSet;
            this.inputGraph = inputGraph;
            this.hasLabelInformation = hasLabelInformation;
            this.partition = partition;
        }

        @Override
        public void run() {
            for (long mappedId = partition.startNode(); mappedId < partition.startNode() + partition.nodeCount(); mappedId++) {
                if (!nodesBitSet.get(mappedId)) {
                    continue;
                }

                long originalId = inputGraph.toOriginalNodeId(mappedId);
                if (hasLabelInformation) {
                    var nodeLabelToken = NodeLabelTokens.of(inputGraph.nodeLabels(mappedId));
                    nodesBuilder.addNode(originalId, nodeLabelToken);
                } else {
                    nodesBuilder.addNode(originalId);
                }
            }
        }
    }
}
