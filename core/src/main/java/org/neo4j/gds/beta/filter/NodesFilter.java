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
package org.neo4j.gds.beta.filter;

import com.carrotsearch.hppc.AbstractIterator;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMapping;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.NodeProperty;
import org.neo4j.gds.api.NodePropertyStore;
import org.neo4j.gds.beta.filter.expression.EvaluationContext;
import org.neo4j.gds.beta.filter.expression.Expression;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.IdMapImplementations;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.DoubleArrayNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.DoubleNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.FloatArrayNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.InnerNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.LongArrayNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.LongNodePropertiesBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeMergeSort;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.paged.SparseLongArray.SUPER_BLOCK_SHIFT;

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
        ProgressTracker progressTracker,
        AllocationTracker allocationTracker
    ) {
        // Depending on the type of the id map we need to construct
        // resolving original and internal ids works different.
        LongToLongFunction originalIdFunction;
        LongToLongFunction internalIdFunction;

        // Partitions over the id space are created depending on the id map
        // implementation. For the BitIdMap, we need to make sure that the
        // ranges of original ids in each partition are aligned with the
        // block size used for creating the BitIdMap. For the regular IdMap,
        // we use range partitioning.
        Iterator<Partition> partitions;

        var inputNodes = graphStore.nodes();

        var nodesBuilderBuilder = GraphFactory.initNodesBuilder()
            .concurrency(concurrency)
            .maxOriginalId(inputNodes.highestNeoId())
            .hasLabelInformation(!graphStore.nodeLabels().isEmpty())
            .tracker(allocationTracker);

        if (IdMapImplementations.useBitIdMap()) {
            // If we need to construct a BitIdMap, we need to make
            // sure that each thread processes a disjoint, ordered
            // subset of original node ids. We therefore produce a
            // temporary array which contains sorted original ids.
            var sortedOriginalIds = sortOriginalIds(
                graphStore,
                concurrency,
                executorService,
                allocationTracker
            );
            // Each thread processes a non-overlapping, consecutive
            // range of node ids. The original id is therefore just
            // a lookup in the sorted array.
            originalIdFunction = sortedOriginalIds::get;
            // The actual internal id is used to lookup node labels
            // and properties. We need to reverse the mapping by
            // looking up the original id in the sorted array and
            // use that original id to retrieve the internal id from
            // the original node mapping.
            internalIdFunction = (id) -> inputNodes.toMappedNodeId(originalIdFunction.applyAsLong(id));
            // We signal the nodes builder to use the block-based
            // BitIdMap builder.
            nodesBuilderBuilder.hasDisjointPartitions(true);
            // Create partitions that are aligned to the blocks that
            // original ids belong to. We must guarantee, that no two
            // partitions contain ids that belong to the same block.
            partitions = PartitionUtils.blockAlignedPartitioning(
                sortedOriginalIds,
                SUPER_BLOCK_SHIFT,
                partition -> partition
            );

        } else {
            // If we need to construct a regular IdMap, we can just
            // delegate to the input node id mapping and use the
            // internal id as given.
            originalIdFunction = inputNodes::toOriginalNodeId;
            internalIdFunction = (id) -> id;

            partitions = PartitionUtils
                .rangePartition(concurrency, graphStore.nodeCount(), Function.identity(), Optional.empty())
                .iterator();
        }

        var nodesBuilder = nodesBuilderBuilder.build();

        var tasks = NodeFilterTask.of(
            graphStore,
            expression,
            partitions,
            originalIdFunction,
            internalIdFunction,
            nodesBuilder,
            progressTracker
        );

        progressTracker.beginSubTask();
        ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);
        progressTracker.endSubTask();

        var nodeMappingAndProperties = nodesBuilder.build();
        var filteredNodeMapping = nodeMappingAndProperties.nodeMapping();

        progressTracker.beginSubTask();
        var filteredNodePropertyStores = filterNodeProperties(
            filteredNodeMapping,
            graphStore,
            concurrency,
            progressTracker
        );
        progressTracker.endSubTask();

        return ImmutableFilteredNodes.builder()
            .nodeMapping(filteredNodeMapping)
            .propertyStores(filteredNodePropertyStores)
            .build();
    }

    private static HugeLongArray sortOriginalIds(
        GraphStore graphStore,
        int concurrency,
        ExecutorService executorService,
        AllocationTracker allocationTracker
    ) {
        var originalIds = HugeLongArray.newArray(graphStore.nodeCount(), allocationTracker);
        var tasks = PartitionUtils.rangePartition(
            concurrency,
            graphStore.nodeCount(),
            partition -> (Runnable) () -> partition.consume(node -> originalIds.set(
                node,
                graphStore.nodes().toOriginalNodeId(node)
            )),
            Optional.empty()
        );
        ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);

        HugeMergeSort.sort(originalIds, concurrency, allocationTracker);

        return originalIds;
    }

    private static Map<NodeLabel, NodePropertyStore> filterNodeProperties(
        NodeMapping filteredNodeMapping,
        GraphStore inputGraphStore,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        return filteredNodeMapping
            .availableNodeLabels()
            .stream()
            .collect(Collectors.toMap(
                Function.identity(),
                nodeLabel -> {
                    var propertyKeys = inputGraphStore.nodePropertyKeys(nodeLabel);

                    return createNodePropertyStore(
                        inputGraphStore,
                        filteredNodeMapping,
                        nodeLabel,
                        propertyKeys,
                        concurrency,
                        progressTracker
                    );
                }
            ));
    }

    private static NodePropertyStore createNodePropertyStore(
        GraphStore inputGraphStore,
        IdMapping filteredNodeMapping,
        NodeLabel nodeLabel,
        Collection<String> propertyKeys,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        progressTracker.beginSubTask();
        progressTracker.setVolume(filteredNodeMapping.nodeCount() * propertyKeys.size());

        var builder = NodePropertyStore.builder();
        var filteredNodeCount = filteredNodeMapping.nodeCount();
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
                    var inputNode = inputMapping.toMappedNodeId(filteredNodeMapping.toOriginalNodeId(filteredNode));
                    nodePropertiesBuilder.accept(inputNode, filteredNode);
                    progressTracker.logProgress();
                }
            );

            builder.putNodeProperty(
                propertyKey,
                NodeProperty.of(propertyKey, propertyState, nodePropertiesBuilder.build(filteredNodeCount))
            );
        });
        progressTracker.endSubTask();
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
        private final ProgressTracker progressTracker;
        private final GraphStore graphStore;
        private final LongToLongFunction originalIdFunction;
        private final LongToLongFunction internalIdFunction;
        private final NodesBuilder nodesBuilder;

        static Iterator<NodeFilterTask> of(
            GraphStore graphStore,
            Expression expression,
            Iterator<Partition> partitions,
            LongToLongFunction originalIdFunction,
            LongToLongFunction internalIdFunction,
            NodesBuilder nodesBuilder,
            ProgressTracker progressTracker
        ) {
            return new AbstractIterator<>() {
                @Override
                protected NodeFilterTask fetch() {
                    if (!partitions.hasNext()) {
                        return done();
                    }

                    return new NodeFilterTask(
                        partitions.next(),
                        expression,
                        graphStore,
                        originalIdFunction,
                        internalIdFunction,
                        nodesBuilder,
                        progressTracker
                    );
                }
            };
        }

        private NodeFilterTask(
            Partition partition,
            Expression expression,
            GraphStore graphStore,
            LongToLongFunction originalIdFunction,
            LongToLongFunction internalIdFunction,
            NodesBuilder nodesBuilder,
            ProgressTracker progressTracker
        ) {
            this.partition = partition;
            this.expression = expression;
            this.graphStore = graphStore;
            this.originalIdFunction = originalIdFunction;
            this.internalIdFunction = internalIdFunction;
            this.nodesBuilder = nodesBuilder;
            this.nodeContext = new EvaluationContext.NodeEvaluationContext(graphStore);
            this.progressTracker = progressTracker;
        }

        @Override
        public void run() {
            var nodeMapping = graphStore.nodes();
            var originalIdFunction = this.originalIdFunction;
            var internalIdFunction = this.internalIdFunction;
            partition.consume(node -> {
                var internalId = internalIdFunction.applyAsLong(node);
                nodeContext.init(internalId);
                if (expression.evaluate(nodeContext) == Expression.TRUE) {
                    var originalId = originalIdFunction.applyAsLong(node);
                    NodeLabel[] labels = nodeMapping.nodeLabels(internalId).toArray(NodeLabel[]::new);
                    nodesBuilder.addNode(originalId, labels);
                }
                progressTracker.logProgress();
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
