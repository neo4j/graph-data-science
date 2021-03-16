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
package org.neo4j.graphalgo.beta.filter.graphstore;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperty;
import org.neo4j.graphalgo.api.NodePropertyStore;
import org.neo4j.graphalgo.beta.filter.expr.EvaluationContext;
import org.neo4j.graphalgo.beta.filter.expr.Expression;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.nodeproperties.DoubleArrayNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.DoubleNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.FloatArrayNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.InnerNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.LongArrayNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.LongNodePropertiesBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Map;
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
        NodeMapping nodeMapping
    ) {
        var nodeContext = new EvaluationContext.NodeEvaluationContext(graphStore);

        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(1)
            .maxOriginalId(graphStore.nodeCount())
            .hasLabelInformation(true)
            .tracker(AllocationTracker.empty())
            .build();

        nodeMapping.forEachNode(node -> {
            nodeContext.init(node);
            if (expression.evaluate(nodeContext) == Expression.TRUE) {
                var neoId = nodeMapping.toOriginalNodeId(node);
                NodeLabel[] labels = graphStore.nodes().nodeLabels(node).toArray(NodeLabel[]::new);
                nodesBuilder.addNode(neoId, labels);
            }
            return true;
        });

        var filteredNodeMapping = nodesBuilder.build();
        var filteredNodePropertyStores = filterNodeProperties(filteredNodeMapping, graphStore);

        return ImmutableFilteredNodes.builder()
            .nodeMapping(filteredNodeMapping)
            .propertyStores(filteredNodePropertyStores)
            .build();
    }

    private static Map<NodeLabel, NodePropertyStore> filterNodeProperties(
        NodeMapping filteredNodeMapping,
        GraphStore inputGraphStore
    ) {
        return filteredNodeMapping
            .availableNodeLabels()
            .stream()
            .collect(Collectors.toMap(
                nodeLabel -> nodeLabel,
                nodeLabel -> {
                    var propertyKeys = inputGraphStore.nodePropertyKeys(nodeLabel);
                    return createNodePropertyStore(inputGraphStore, filteredNodeMapping, nodeLabel, propertyKeys);
                })
            );
    }

    private static NodePropertyStore createNodePropertyStore(
        GraphStore inputGraphStore,
        NodeMapping filteredMapping,
        NodeLabel nodeLabel,
        Iterable<String> propertyKeys
    ) {
        var builder = NodePropertyStore.builder();
        var filteredNodeCount = filteredMapping.nodeCount();
        var inputMapping = inputGraphStore.nodes();

        var allocationTracker = AllocationTracker.empty();

        // TODO: parallel?
        propertyKeys.forEach(propertyKey -> {
            var nodeProperties = inputGraphStore.nodePropertyValues(nodeLabel, propertyKey);
            var propertyState = inputGraphStore.nodePropertyState(propertyKey);
            var propertyType = nodeProperties.valueType();


            InnerNodePropertiesBuilder propertiesBuilder = getPropertiesBuilder(
                filteredNodeCount,
                allocationTracker,
                propertyType
            );

            filteredMapping.forEachNode(filteredNode -> {
                var inputNode = inputMapping.toMappedNodeId(filteredMapping.toOriginalNodeId(filteredNode));
                // TODO: can we get rid of the `value` conversion here?
                propertiesBuilder.setValue(filteredNode, nodeProperties.value(inputNode));
                return true;
            });

            builder.putNodeProperty(
                propertyKey,
                NodeProperty.of(propertyKey, propertyState, propertiesBuilder.build(filteredNodeCount))
            );
        });

        return builder.build();
    }

    private static InnerNodePropertiesBuilder getPropertiesBuilder(
        long filteredNodeCount,
        AllocationTracker allocationTracker,
        org.neo4j.graphalgo.api.nodeproperties.ValueType propertyType
    ) {
        InnerNodePropertiesBuilder propertiesBuilder = null;
        switch (propertyType) {
            case LONG:
                propertiesBuilder = new LongNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forLong(),
                    allocationTracker
                );
                break;
            case DOUBLE:
                propertiesBuilder = new DoubleNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forDouble(),
                    allocationTracker
                );
                break;
            case DOUBLE_ARRAY:
                propertiesBuilder = new DoubleArrayNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forDoubleArray(),
                    allocationTracker
                );
                break;
            case FLOAT_ARRAY:
                propertiesBuilder = new FloatArrayNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forFloatArray(),
                    allocationTracker
                );
                break;
            case LONG_ARRAY:
                propertiesBuilder = new LongArrayNodePropertiesBuilder(
                    filteredNodeCount,
                    DefaultValue.forLongArray(),
                    allocationTracker
                );
                break;
            case UNKNOWN:
                throw new UnsupportedOperationException("Cannot import properties of type UNKNOWN");
        }

        return propertiesBuilder;
    }

    private NodesFilter() {}
}
