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
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.NodeProperty;
import org.neo4j.graphalgo.api.NodePropertyStore;
import org.neo4j.graphalgo.api.RelationshipProperty;
import org.neo4j.graphalgo.api.RelationshipPropertyStore;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.beta.filter.expr.EvaluationContext;
import org.neo4j.graphalgo.beta.filter.expr.Expression;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.CSRGraphStore;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.values.storable.NumberType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class GraphStoreFilter {

    private GraphStoreFilter() {}

    @NotNull
    public static GraphStore subGraph(GraphStore graphStore, Expression nodeExpr, Expression relationshipExpr) {
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

    static Map<NodeLabel, NodePropertyStore> filterNodeProperties(NodeMapping nodeMapping, GraphStore graphStore) {
        return graphStore.nodePropertyKeys()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> nodePropertyStore(nodeMapping, entry.getKey(), entry.getValue(), graphStore)
            ));
    }

    static NodePropertyStore nodePropertyStore(
        NodeMapping filteredMapping,
        NodeLabel nodeLabel,
        Set<String> propertyKeys,
        GraphStore graphStore
    ) {
        var builder = NodePropertyStore.builder();
        var filteredNodeCount = filteredMapping.nodeCount();
        var inputMapping = graphStore.nodes();

        var allocationTracker = AllocationTracker.empty();

        // TODO: parallel?
        propertyKeys.forEach(propertyKey -> {
            var nodeProperties = graphStore.nodePropertyValues(nodeLabel, propertyKey);
            var propertyState = graphStore.nodePropertyState(propertyKey);
            var propertyType = nodeProperties.valueType();


            NodeProperties filteredNodeProperties = null;

            switch (propertyType) {
                case LONG:
                    var hla = HugeLongArray.newArray(filteredNodeCount, allocationTracker);
                    filteredMapping.forEachNode(filteredNode -> {
                        var inputNode = inputMapping.toMappedNodeId(filteredMapping.toOriginalNodeId(filteredNode));
                        hla.set(filteredNode, nodeProperties.longValue(inputNode));
                        return true;
                    });
                    filteredNodeProperties = hla.asNodeProperties();
                    break;
                case DOUBLE:
                    var hda = HugeDoubleArray.newArray(filteredNodeCount, allocationTracker);
                    filteredMapping.forEachNode(filteredNode -> {
                        var inputNode = inputMapping.toMappedNodeId(filteredMapping.toOriginalNodeId(filteredNode));
                        hda.set(filteredNode, nodeProperties.doubleValue(inputNode));
                        return true;
                    });
                    filteredNodeProperties = hda.asNodeProperties();
                    break;
                case DOUBLE_ARRAY:
                    break;
                case FLOAT_ARRAY:
                    break;
                case LONG_ARRAY:
                    break;
                case UNKNOWN:
                    break;
            }

            builder.putNodeProperty(
                propertyKey,
                NodeProperty.of(propertyKey, propertyState, filteredNodeProperties)
            );
        });

        return builder.build();
    }
}
