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

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperty;
import org.neo4j.graphalgo.api.NodePropertyStore;
import org.neo4j.graphalgo.api.RelationshipProperty;
import org.neo4j.graphalgo.api.RelationshipPropertyStore;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.beta.filter.expr.EvaluationContext;
import org.neo4j.graphalgo.beta.filter.expr.Expression;
import org.neo4j.graphalgo.beta.filter.expr.ExpressionParser;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.CSRGraphStore;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.nodeproperties.DoubleArrayNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.DoubleNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.FloatArrayNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.InnerNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.LongArrayNodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.LongNodePropertiesBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.values.storable.NumberType;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class GraphStoreFilter {

    private GraphStoreFilter() {}

    @NotNull
    public static GraphStore subGraph(GraphStore graphStore, String nodeFilter, String relationshipFilter)
    throws ParseException {
        var nodeExpr = ExpressionParser.parse(nodeFilter);
        var relationshipExpr = ExpressionParser.parse(relationshipFilter);

        var nodeContext = new EvaluationContext.NodeEvaluationContext(graphStore);
        var relationshipContext = new EvaluationContext.RelationshipEvaluationContext(Map.of());
        var inputNodes = graphStore.nodes();

        NodeMapping outputNodes = filterNodes(graphStore, nodeExpr, nodeContext, inputNodes);

        Map<RelationshipType, Relationships.Topology> topologies = new HashMap<>();
        Map<RelationshipType, RelationshipPropertyStore> relPropertyStores = new HashMap<>();

        for (RelationshipType relType : graphStore.relationshipTypes()) {
            Relationships outputRelationships = filterRelationships(
                graphStore,
                relationshipExpr,
                relationshipContext,
                inputNodes,
                outputNodes,
                relType
            );

            topologies.put(relType, outputRelationships.topology());
            outputRelationships.properties().ifPresent(properties -> {
                var propertyKey = graphStore.relationshipPropertyKeys(relType).stream().findFirst().get();
                relPropertyStores.put(
                    relType,
                    RelationshipPropertyStore.builder()
                        .putIfAbsent(
                            propertyKey,
                            RelationshipProperty.of(
                                propertyKey,
                                NumberType.FLOATING_POINT,
                                GraphStore.PropertyState.TRANSIENT,
                                properties,
                                DefaultValue.forDouble(),
                                Aggregation.NONE
                            )
                        ).build()
                );
            });
        }

        var nodeProperties = filterNodeProperties(outputNodes, graphStore);

        return CSRGraphStore.of(
            graphStore.databaseId(),
            outputNodes,
            nodeProperties,
            topologies,
            relPropertyStores,
            1,
            AllocationTracker.empty()
        );
    }

    static NodeMapping filterNodes(
        GraphStore graphStore,
        Expression nodeExpr,
        EvaluationContext.NodeEvaluationContext nodeContext,
        NodeMapping inputNodes
    ) {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(1)
            .maxOriginalId(graphStore.nodeCount())
            .hasLabelInformation(true)
            .tracker(AllocationTracker.empty())
            .build();

        inputNodes.forEachNode(node -> {
            nodeContext.init(node);
            if (nodeExpr.evaluate(nodeContext) == Expression.TRUE) {
                var neoId = inputNodes.toOriginalNodeId(node);
                NodeLabel[] labels = graphStore.nodes().nodeLabels(node).toArray(NodeLabel[]::new);
                nodesBuilder.addNode(neoId, labels);
            }
            return true;
        });

        return nodesBuilder.build();
    }

    static Relationships filterRelationships(
        GraphStore graphStore,
        Expression relationshipExpr,
        EvaluationContext.RelationshipEvaluationContext relationshipContext,
        NodeMapping inputNodes,
        NodeMapping outputNodes,
        RelationshipType relType
    ) {
        // TODO use CompositeRelationshipIterator
        var propertyKeys = graphStore.relationshipPropertyKeys(relType);

        if (propertyKeys.isEmpty()) {
            var graph = graphStore.getGraph(relType);
            return filterRelationshipsNoProperties(
                relationshipExpr,
                relationshipContext,
                inputNodes,
                outputNodes,
                relType,
                graph
            );
        } else {
            var propertyKey = propertyKeys.stream().findFirst().get();
            var graph = graphStore.getGraph(relType, Optional.of(propertyKey));

            var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(outputNodes)
                .concurrency(1)
                .addPropertyConfig(Aggregation.NONE, DefaultValue.forDouble())
                .tracker(AllocationTracker.empty())
                .build();

            outputNodes.forEachNode(node -> {
                var neoSource = outputNodes.toOriginalNodeId(node);

                graph.forEachRelationship(neoSource, Double.NaN, (sourceNodeId, targetNodeId, property) -> {
                    var neoTarget = inputNodes.toOriginalNodeId(targetNodeId);
                    var mappedTarget = outputNodes.toMappedNodeId(neoTarget);

                    if (mappedTarget != -1) {
                        double[] properties = {property};
                        relationshipContext.init(relType.name, properties);
                        if (relationshipExpr.evaluate(relationshipContext) == Expression.TRUE) {
                            relationshipsBuilder.add(neoSource, neoTarget, property);
                        }
                    }
                    return true;
                });
                return true;
            });

            // TODO build all in multi property case
            return relationshipsBuilder.build();
        }
    }

    static Relationships filterRelationshipsNoProperties(
        Expression relationshipExpr,
        EvaluationContext.RelationshipEvaluationContext relationshipContext,
        NodeMapping inputNodes,
        NodeMapping outputNodes,
        RelationshipType relType,
        org.neo4j.graphalgo.api.Graph graph
    ) {
        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(outputNodes)
            .concurrency(1)
            .tracker(AllocationTracker.empty())
            .build();

        outputNodes.forEachNode(node -> {
            var neoSource = outputNodes.toOriginalNodeId(node);

            graph.forEachRelationship(neoSource, (sourceNodeId, targetNodeId) -> {
                var neoTarget = inputNodes.toOriginalNodeId(targetNodeId);
                var mappedTarget = outputNodes.toMappedNodeId(neoTarget);

                if (mappedTarget != -1) {
                    relationshipContext.init(relType.name);
                    if (relationshipExpr.evaluate(relationshipContext) == Expression.TRUE) {
                        relationshipsBuilder.add(neoSource, neoTarget);
                    }
                }
                return true;
            });
            return true;
        });

        return relationshipsBuilder.build();
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
                    return nodePropertyStore(filteredNodeMapping, nodeLabel, propertyKeys, inputGraphStore);
                })
            );
    }

    private static NodePropertyStore nodePropertyStore(
        NodeMapping filteredMapping,
        NodeLabel nodeLabel,
        Set<String> propertyKeys,
        GraphStore inputGraphStore
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
}
