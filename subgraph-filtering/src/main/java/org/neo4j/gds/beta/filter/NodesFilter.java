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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.beta.filter.expression.EvaluationContext;
import org.neo4j.gds.beta.filter.expression.Expression;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.DoubleArrayNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.DoubleNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.FloatArrayNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.InnerNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.LongArrayNodePropertiesBuilder;
import org.neo4j.gds.core.loading.nodeproperties.LongNodePropertiesBuilder;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public final class NodesFilter {

    @ValueClass
    interface FilteredNodes {
        IdMap idMap();

        NodePropertyStore propertyStores();
    }

    static FilteredNodes filterNodes(
        GraphStore inputGraphStore,
        Expression expression,
        int concurrency,
        Map<String, Object> parameterMap,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        var inputNodes = inputGraphStore.nodes();

        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(concurrency)
            .maxOriginalId(inputNodes.highestOriginalId())
            .hasLabelInformation(!inputGraphStore.nodeLabels().isEmpty())
            .build();

        var partitions = PartitionUtils
            .rangePartition(concurrency, inputGraphStore.nodeCount(), Function.identity(), Optional.empty())
            .iterator();

        var tasks = NodeFilterTask.of(
            inputGraphStore,
            expression,
            parameterMap,
            partitions,
            nodesBuilder,
            progressTracker
        );

        progressTracker.beginSubTask();
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executorService)
            .run();
        progressTracker.endSubTask();

        var idMapAndProperties = nodesBuilder.build();
        var filteredIdMap = idMapAndProperties.idMap();

        progressTracker.beginSubTask();
        var filteredNodePropertyStores = filterNodeProperties(
            inputGraphStore,
            filteredIdMap,
            concurrency,
            progressTracker
        );
        progressTracker.endSubTask();

        return ImmutableFilteredNodes.builder()
            .idMap(filteredIdMap)
            .propertyStores(filteredNodePropertyStores)
            .build();
    }

    public static NodePropertyStore filterNodeProperties(
        GraphStore inputGraphStore,
        IdMap filteredIdMap,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        var propertyKeys = inputGraphStore.nodePropertyKeys();

        progressTracker.beginSubTask(filteredIdMap.nodeCount() * propertyKeys.size());

        var builder = NodePropertyStore.builder();
        var filteredNodeCount = filteredIdMap.nodeCount();
        var inputMapping = inputGraphStore.nodes();

        propertyKeys.forEach(propertyKey -> {
            var nodeProperty = inputGraphStore.nodeProperty(propertyKey);

            NodePropertiesBuilder<?> nodePropertiesBuilder = getPropertiesBuilder(
                inputMapping,
                nodeProperty.values(),
                concurrency
            );

            ParallelUtil.parallelForEachNode(
                filteredNodeCount,
                concurrency,
                filteredNode -> {
                    var inputNode = inputMapping.toMappedNodeId(filteredIdMap.toOriginalNodeId(filteredNode));
                    nodePropertiesBuilder.accept(inputNode, filteredNode);
                    progressTracker.logProgress();
                }
            );

            builder.putProperty(
                propertyKey,
                NodeProperty.of(
                    propertyKey,
                    nodeProperty.propertyState(),
                    nodePropertiesBuilder.build(filteredNodeCount, filteredIdMap)
                )
            );
        });
        progressTracker.endSubTask();
        return builder.build();
    }

    public static NodePropertiesBuilder<?> getPropertiesBuilder(
        IdMap idMap,
        NodePropertyValues inputNodePropertyValues,
        int concurrency
    ) {
        NodePropertiesBuilder<?> propertiesBuilder = null;

        switch (inputNodePropertyValues.valueType()) {
            case LONG:
                var longNodePropertiesBuilder = LongNodePropertiesBuilder.of(
                    DefaultValue.forLong(),
                    concurrency
                );
                propertiesBuilder = new NodePropertiesBuilder<>(inputNodePropertyValues, longNodePropertiesBuilder) {
                    @Override
                    public void accept(long inputNode, long filteredNode) {

                        propertyBuilder.set(idMap.toOriginalNodeId(inputNode), inputProperties.longValue(inputNode));
                    }
                };
                break;

            case DOUBLE:
                var doubleNodePropertiesBuilder = new DoubleNodePropertiesBuilder(
                    DefaultValue.forDouble(),
                    concurrency
                );
                propertiesBuilder = new NodePropertiesBuilder<>(inputNodePropertyValues, doubleNodePropertiesBuilder) {
                    @Override
                    public void accept(long inputNode, long filteredNode) {
                        propertyBuilder.set(idMap.toOriginalNodeId(inputNode), inputProperties.doubleValue(inputNode));
                    }
                };
                break;

            case DOUBLE_ARRAY:
                var doubleArrayNodePropertiesBuilder = new DoubleArrayNodePropertiesBuilder(
                    DefaultValue.forDoubleArray(),
                    concurrency
                );
                propertiesBuilder = new NodePropertiesBuilder<>(inputNodePropertyValues, doubleArrayNodePropertiesBuilder) {
                    @Override
                    public void accept(long inputNode, long filteredNode) {
                        propertyBuilder.set(idMap.toOriginalNodeId(inputNode), inputProperties.doubleArrayValue(inputNode));
                    }
                };
                break;

            case FLOAT_ARRAY:
                var floatArrayNodePropertiesBuilder = new FloatArrayNodePropertiesBuilder(
                    DefaultValue.forFloatArray(),
                    concurrency
                );

                propertiesBuilder = new NodePropertiesBuilder<>(inputNodePropertyValues, floatArrayNodePropertiesBuilder) {
                    @Override
                    public void accept(long inputNode, long filteredNode) {
                        propertyBuilder.set(idMap.toOriginalNodeId(inputNode), inputProperties.floatArrayValue(inputNode));
                    }
                };
                break;

            case LONG_ARRAY:
                var longArrayNodePropertiesBuilder = new LongArrayNodePropertiesBuilder(
                    DefaultValue.forFloatArray(),
                    concurrency
                );

                propertiesBuilder = new NodePropertiesBuilder<>(inputNodePropertyValues, longArrayNodePropertiesBuilder) {
                    @Override
                    public void accept(long inputNode, long filteredNode) {
                        propertyBuilder.set(idMap.toOriginalNodeId(inputNode), inputProperties.longArrayValue(inputNode));
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
        private final GraphStore inputGraphStore;
        private final NodesBuilder nodesBuilder;

        static Iterator<NodeFilterTask> of(
            GraphStore inputGraphStore,
            Expression expression,
            Map<String, Object> parameterMap,
            Iterator<Partition> partitions,
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
                        parameterMap,
                        inputGraphStore,
                        nodesBuilder,
                        progressTracker
                    );
                }
            };
        }

        private NodeFilterTask(
            Partition partition,
            Expression expression,
            Map<String, Object> parameterMap,
            GraphStore inputGraphStore,
            NodesBuilder nodesBuilder,
            ProgressTracker progressTracker
        ) {
            this.partition = partition;
            this.expression = expression;
            this.inputGraphStore = inputGraphStore;
            this.nodesBuilder = nodesBuilder;
            this.nodeContext = new EvaluationContext.NodeEvaluationContext(inputGraphStore, parameterMap);
            this.progressTracker = progressTracker;
        }

        @Override
        public void run() {
            var idMap = inputGraphStore.nodes();
            partition.consume(node -> {
                nodeContext.init(node);
                if (expression.evaluate(nodeContext) == Expression.TRUE) {
                    var originalId = idMap.toOriginalNodeId(node);
                    var labels = NodeLabelTokens.of(idMap.nodeLabels(node));
                    nodesBuilder.addNode(originalId, labels);
                }
                progressTracker.logProgress();
            });
        }
    }

    public abstract static class NodePropertiesBuilder<T extends InnerNodePropertiesBuilder> {
        final NodePropertyValues inputProperties;
        final T propertyBuilder;

        NodePropertiesBuilder(NodePropertyValues inputProperties, T propertyBuilder) {
            this.inputProperties = inputProperties;
            this.propertyBuilder = propertyBuilder;
        }

        public abstract void accept(long inputNode, long filteredNode);

        public NodePropertyValues build(long size, IdMap idMap) {
            return propertyBuilder.build(size, idMap);
        }
    }
}
