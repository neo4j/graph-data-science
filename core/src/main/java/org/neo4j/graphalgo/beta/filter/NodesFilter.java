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

import org.apache.commons.lang3.mutable.MutableInt;
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
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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
        ProgressLogger progressLogger,
        AllocationTracker allocationTracker
    ) {
        progressLogger.startSubTask("Nodes").reset(graphStore.nodeCount());

        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(concurrency)
            .maxOriginalId(graphStore.nodes().highestNeoId())
            .hasLabelInformation(!graphStore.nodeLabels().isEmpty())
            .tracker(allocationTracker)
            .build();

        var nodeFilterTasks = PartitionUtils.rangePartition(
            concurrency,
            graphStore.nodeCount(),
            partition -> new NodeFilterTask(partition, expression, graphStore, nodesBuilder, progressLogger)
        );

        ParallelUtil.runWithConcurrency(concurrency, nodeFilterTasks, executorService);

        var nodeMappingAndProperties = nodesBuilder.build();
        var filteredNodeMapping = nodeMappingAndProperties.nodeMapping();

        progressLogger
            .finishSubTask("Nodes")
            .startSubTask("Node properties");

        var filteredNodePropertyStores = filterNodeProperties(
            filteredNodeMapping,
            graphStore,
            executorService,
            concurrency,
            progressLogger
        );

        progressLogger.finishSubTask("Node properties");

        return ImmutableFilteredNodes.builder()
            .nodeMapping(filteredNodeMapping)
            .propertyStores(filteredNodePropertyStores)
            .build();
    }

    private static Map<NodeLabel, NodePropertyStore> filterNodeProperties(
        NodeMapping filteredNodeMapping,
        GraphStore inputGraphStore,
        ExecutorService executorService,
        int concurrency,
        ProgressLogger progressLogger
    ) {
        var totalLabelCount = filteredNodeMapping.availableNodeLabels().size();
        var current = new MutableInt(1);

        return filteredNodeMapping
            .availableNodeLabels()
            .stream()
            .collect(Collectors.toMap(
                nodeLabel -> nodeLabel,
                nodeLabel -> {
                    var propertyKeys = inputGraphStore.nodePropertyKeys(nodeLabel);

                    var taskName = formatWithLocale("Label %d of %d", current.getAndIncrement(), totalLabelCount);
                    progressLogger.startSubTask(taskName);

                    var nodePropertyStore = createNodePropertyStore(
                        inputGraphStore,
                        filteredNodeMapping,
                        nodeLabel,
                        propertyKeys,
                        concurrency,
                        executorService,
                        progressLogger
                    );

                    progressLogger.finishSubTask(taskName);

                    return nodePropertyStore;
                }
                )
            );
    }

    private static NodePropertyStore createNodePropertyStore(
        GraphStore inputGraphStore,
        NodeMapping filteredMapping,
        NodeLabel nodeLabel,
        Collection<String> propertyKeys,
        int concurrency,
        ExecutorService executorService,
        ProgressLogger progressLogger
    ) {
        var builder = NodePropertyStore.builder();
        var filteredNodeCount = filteredMapping.nodeCount();
        var inputMapping = inputGraphStore.nodes();

        var allocationTracker = AllocationTracker.empty();

        var propertyCount = propertyKeys.size();
        var current = new MutableInt(1);

        propertyKeys.forEach(propertyKey -> {
            var nodeProperties = inputGraphStore.nodePropertyValues(nodeLabel, propertyKey);
            var propertyState = inputGraphStore.nodePropertyState(propertyKey);

            NodePropertiesBuilder<?> nodePropertiesBuilder = getPropertiesBuilder(
                filteredNodeCount,
                allocationTracker,
                nodeProperties
            );

            var taskMessage = formatWithLocale("Property %d of %d", current.getAndIncrement(), propertyCount);
            progressLogger.startSubTask(taskMessage).reset(filteredNodeCount);

            ParallelUtil.parallelForEachNode(
                filteredNodeCount,
                concurrency,
                filteredNode -> {
                    var inputNode = inputMapping.toMappedNodeId(filteredMapping.toOriginalNodeId(filteredNode));
                    nodePropertiesBuilder.accept(inputNode, filteredNode);
                    progressLogger.logProgress();
                }
            );

            progressLogger.finishSubTask(taskMessage);

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
        private final Partition partition;
        private final Expression expression;
        private final EvaluationContext.NodeEvaluationContext nodeContext;
        private final ProgressLogger progressLogger;
        private final GraphStore graphStore;
        private final NodesBuilder nodesBuilder;

        private NodeFilterTask(
            Partition partition,
            Expression expression,
            GraphStore graphStore,
            NodesBuilder nodesBuilder,
            ProgressLogger progressLogger
        ) {
            this.partition = partition;
            this.expression = expression;
            this.graphStore = graphStore;
            this.nodesBuilder = nodesBuilder;

            this.nodeContext = new EvaluationContext.NodeEvaluationContext(graphStore);
            this.progressLogger = progressLogger;
        }

        @Override
        public void run() {
            var nodeMapping = graphStore.nodes();

            partition.consume(node -> {
                nodeContext.init(node);
                if (expression.evaluate(nodeContext) == Expression.TRUE) {
                    var neoId = nodeMapping.toOriginalNodeId(node);
                    NodeLabel[] labels = nodeMapping.nodeLabels(node).toArray(NodeLabel[]::new);
                    nodesBuilder.addNode(neoId, labels);
                }
                progressLogger.logProgress();
            });
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
