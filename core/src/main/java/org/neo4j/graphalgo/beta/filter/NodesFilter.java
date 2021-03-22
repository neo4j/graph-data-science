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
package org.neo4j.graphalgo.beta.filter;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.NodeProperty;
import org.neo4j.graphalgo.api.NodePropertyStore;
import org.neo4j.graphalgo.beta.filter.expression.EvaluationContext;
import org.neo4j.graphalgo.beta.filter.expression.Expression;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.NodesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.DoubleArrayNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.DoubleNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.FloatArrayNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.InnerNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.LongArrayNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.LongNodePropertiesBuilder;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

final class NodesFilter {

    @ValueClass
    interface FilteredNodes {
        NodeMapping nodeMapping();

        Map<NodeLabel, NodePropertyStore> propertyStores();
    }

    static FilteredNodes filterNodes(
        GraphStore graphStore,
        Expression expression,
        int concurrency,
        ExecutorService executorService,
        AllocationTracker allocationTracker
    ) {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(concurrency)
            .maxOriginalId(graphStore.nodeCount())
            .hasLabelInformation(!graphStore.nodeLabels().isEmpty())
            .tracker(allocationTracker)
            .build();

        var batchSize = BitUtil.ceilDiv(graphStore.nodeCount(), concurrency);
        var nodeFilterTasks = graphStore
            .nodes()
            .batchIterables(batchSize)
            .stream()
            .map(nodeIterator -> new NodeFilterTask(nodeIterator, expression, graphStore, nodesBuilder))
            .collect(Collectors.toList());

        ParallelUtil.runWithConcurrency(concurrency, nodeFilterTasks, executorService);

        var filteredNodeMapping = nodesBuilder.build();
        var filteredNodePropertyStores = filterNodeProperties(filteredNodeMapping, graphStore, concurrency);

        return ImmutableFilteredNodes.builder()
            .nodeMapping(filteredNodeMapping)
            .propertyStores(filteredNodePropertyStores)
            .build();
    }

    private static Map<NodeLabel, NodePropertyStore> filterNodeProperties(
        NodeMapping filteredNodeMapping,
        GraphStore inputGraphStore,
        int concurrency
    ) {
        return filteredNodeMapping
            .availableNodeLabels()
            .stream()
            .collect(Collectors.toMap(
                nodeLabel -> nodeLabel,
                nodeLabel -> {
                    var propertyKeys = inputGraphStore.nodePropertyKeys(nodeLabel);
                    return createNodePropertyStore(
                        inputGraphStore,
                        filteredNodeMapping,
                        nodeLabel,
                        propertyKeys,
                        concurrency
                    );
                }
                )
            );
    }

    private static NodePropertyStore createNodePropertyStore(
        GraphStore inputGraphStore,
        NodeMapping filteredMapping,
        NodeLabel nodeLabel,
        Iterable<String> propertyKeys,
        int concurrency
    ) {
        var builder = NodePropertyStore.builder();
        var filteredNodeCount = filteredMapping.nodeCount();
        var inputMapping = inputGraphStore.nodes();

        var allocationTracker = AllocationTracker.empty();

        propertyKeys.forEach(propertyKey -> {
            var nodeProperties = inputGraphStore.nodePropertyValues(nodeLabel, propertyKey);
            var propertyState = inputGraphStore.nodePropertyState(propertyKey);

            NodePropertiesBuilder<?> nodePropertiesBuilder = getPropertiesBuilder(
                filteredNodeCount,
                allocationTracker,
                nodeProperties
            );

            ParallelUtil.parallelForEachNode(
                filteredNodeCount,
                concurrency,
                filteredNode -> {
                    var inputNode = inputMapping.toMappedNodeId(filteredMapping.toOriginalNodeId(filteredNode));
                    nodePropertiesBuilder.accept(inputNode, filteredNode);
                }
            );

            builder.putNodeProperty(
                propertyKey,
                NodeProperty.of(propertyKey, propertyState, nodePropertiesBuilder.build(filteredNodeCount))
            );
        });

        return builder.build();
    }

    private static NodePropertiesBuilder<?> getPropertiesBuilder(
        long filteredNodeCount,
        AllocationTracker allocationTracker,
        NodeProperties inputNodeProperties
    ) {
        NodePropertiesBuilder<?> propertiesBuilder = null;

        switch (inputNodeProperties.valueType()) {
            case LONG:
                var longNodePropertiesBuilder = new LongNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forLong(),
                    allocationTracker
                );
                propertiesBuilder = new NodePropertiesBuilder<>(inputNodeProperties, longNodePropertiesBuilder) {
                    @Override
                    void accept(long inputNode, long filteredNode) {
                        propertyBuilder.set(filteredNode, inputProperties.longValue(inputNode));
                    }
                };
                break;

            case DOUBLE:
                var doubleNodePropertiesBuilder = new DoubleNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forDouble(),
                    allocationTracker
                );
                propertiesBuilder = new NodePropertiesBuilder<>(inputNodeProperties, doubleNodePropertiesBuilder) {
                    @Override
                    void accept(long inputNode, long filteredNode) {
                        propertyBuilder.set(filteredNode, inputProperties.doubleValue(inputNode));
                    }
                };
                break;

            case DOUBLE_ARRAY:
                var doubleArrayNodePropertiesBuilder = new DoubleArrayNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forDoubleArray(),
                    allocationTracker
                );
                propertiesBuilder = new NodePropertiesBuilder<>(inputNodeProperties, doubleArrayNodePropertiesBuilder) {
                    @Override
                    void accept(long inputNode, long filteredNode) {
                        propertyBuilder.set(filteredNode, inputProperties.doubleArrayValue(inputNode));
                    }
                };
                break;

            case FLOAT_ARRAY:
                var floatArrayNodePropertiesBuilder = new FloatArrayNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forFloatArray(),
                    allocationTracker
                );

                propertiesBuilder = new NodePropertiesBuilder<>(inputNodeProperties, floatArrayNodePropertiesBuilder) {
                    @Override
                    void accept(long inputNode, long filteredNode) {
                        propertyBuilder.set(filteredNode, inputProperties.floatArrayValue(inputNode));
                    }
                };
                break;

            case LONG_ARRAY:
                var longArrayNodePropertiesBuilder = new LongArrayNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forLongArray(),
                    allocationTracker
                );

                propertiesBuilder = new NodePropertiesBuilder<>(inputNodeProperties, longArrayNodePropertiesBuilder) {
                    @Override
                    void accept(long inputNode, long filteredNode) {
                        propertyBuilder.set(filteredNode, inputProperties.longArrayValue(inputNode));
                    }
                };
                break;

            case UNKNOWN:
                throw new UnsupportedOperationException("Cannot import properties of type UNKNOWN");
        }
        return propertiesBuilder;
    }

    private NodesFilter() {}

    private static final class NodeFilterTask implements Runnable {
        private final PrimitiveLongIterator nodeIterator;
        private final Expression expression;
        private final EvaluationContext.NodeEvaluationContext nodeContext;
        private final GraphStore graphStore;
        private final NodesBuilder nodesBuilder;

        private NodeFilterTask(
            PrimitiveLongIterable nodeIterator,
            Expression expression,
            GraphStore graphStore,
            NodesBuilder nodesBuilder
        ) {
            this.nodeIterator = nodeIterator.iterator();
            this.expression = expression;
            this.graphStore = graphStore;
            this.nodesBuilder = nodesBuilder;

            this.nodeContext = new EvaluationContext.NodeEvaluationContext(graphStore);
        }


        @Override
        public void run() {
            long node;
            var nodeMapping = graphStore.nodes();

            while (nodeIterator.hasNext()) {
                node = nodeIterator.next();
                nodeContext.init(node);
                if (expression.evaluate(nodeContext) == Expression.TRUE) {
                    var neoId = nodeMapping.toOriginalNodeId(node);
                    NodeLabel[] labels = nodeMapping.nodeLabels(node).toArray(NodeLabel[]::new);
                    nodesBuilder.addNode(neoId, labels);
                }
            }
        }
    }

    private abstract static class NodePropertiesBuilder<T extends InnerNodePropertiesBuilder> {
        final NodeProperties inputProperties;
        final T propertyBuilder;

        NodePropertiesBuilder(NodeProperties inputProperties, T propertyBuilder) {
            this.inputProperties = inputProperties;
            this.propertyBuilder = propertyBuilder;
        }

        abstract void accept(long inputNode, long filteredNode);

        NodeProperties build(long size) {
            return propertyBuilder.build(size);
        }
    }
}
