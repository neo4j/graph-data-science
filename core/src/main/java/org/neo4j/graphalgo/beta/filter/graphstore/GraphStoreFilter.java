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
import org.neo4j.graphalgo.annotation.ValueClass;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.api.AdjacencyCursor.NOT_FOUND;

public final class GraphStoreFilter {

    private GraphStoreFilter() {}

    @NotNull
    public static GraphStore subGraph(GraphStore graphStore, String nodeFilter, String relationshipFilter)
    throws ParseException {
        var nodeExpr = ExpressionParser.parse(nodeFilter);
        var relationshipExpr = ExpressionParser.parse(relationshipFilter);

        var inputNodes = graphStore.nodes();

        var filteredNodes = filterNodes(graphStore, nodeExpr, inputNodes);
        var filteredRelationships = filterRelationships(
            graphStore,
            relationshipExpr,
            inputNodes,
            filteredNodes.nodeMapping()
        );

        return CSRGraphStore.of(
            graphStore.databaseId(),
            filteredNodes.nodeMapping(),
            filteredNodes.propertyStores(),
            filteredRelationships.topology(),
            filteredRelationships.propertyStores(),
            // TODO
            1,
            AllocationTracker.empty()
        );
    }

    @ValueClass
    interface FilteredNodes {
        NodeMapping nodeMapping();

        Map<NodeLabel, NodePropertyStore> propertyStores();
    }

    private static FilteredNodes filterNodes(
        GraphStore graphStore,
        Expression nodeExpr,
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
            if (nodeExpr.evaluate(nodeContext) == Expression.TRUE) {
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

    @ValueClass
    interface FilteredRelationships {

        Map<RelationshipType, Relationships.Topology> topology();

        Map<RelationshipType, RelationshipPropertyStore> propertyStores();
    }

    private static FilteredRelationships filterRelationships(
        GraphStore graphStore,
        Expression relationshipExpr,
        NodeMapping inputNodes,
        NodeMapping outputNodes
    ) {
        Map<RelationshipType, Relationships.Topology> topologies = new HashMap<>();
        Map<RelationshipType, RelationshipPropertyStore> relPropertyStores = new HashMap<>();

        for (RelationshipType relType : graphStore.relationshipTypes()) {
            var outputRelationships = filterRelationshipType(
                graphStore,
                relationshipExpr,
                inputNodes,
                outputNodes,
                relType
            );

            // Drop relationship types that have been completely filtered out.
            if (outputRelationships.topology().elementCount() == 0) {
                continue;
            }

            topologies.put(relType, outputRelationships.topology());

            var propertyStoreBuilder = RelationshipPropertyStore.builder();
            outputRelationships.properties().forEach((propertyKey, properties) -> {
                propertyStoreBuilder.putIfAbsent(
                    propertyKey,
                    RelationshipProperty.of(
                        propertyKey,
                        NumberType.FLOATING_POINT,
                        GraphStore.PropertyState.PERSISTENT,
                        properties,
                        DefaultValue.forDouble(),
                        Aggregation.NONE
                    )
                );
            });

            relPropertyStores.put(relType, propertyStoreBuilder.build());
        }

        // If all relationship types have been filtered, we need to add a dummy
        // topology in order to be able to create a graph store.
        // TODO: could live in GraphStore factory method
        if (topologies.isEmpty()) {
            var emptyTopology = GraphFactory.initRelationshipsBuilder()
                .nodes(outputNodes)
                .concurrency(1)
                .tracker(AllocationTracker.empty())
                .build()
                .build()
                .topology();

            topologies.put(RelationshipType.ALL_RELATIONSHIPS, emptyTopology);
        }

        return ImmutableFilteredRelationships.builder()
            .topology(topologies)
            .propertyStores(relPropertyStores)
            .build();
    }

    @ValueClass
    interface FilteredRelationship {
        RelationshipType relationshipType();

        Relationships.Topology topology();

        Map<String, Relationships.Properties> properties();
    }

    private static FilteredRelationship filterRelationshipType(
        GraphStore graphStore,
        Expression relationshipExpr,
        NodeMapping inputNodes,
        NodeMapping outputNodes,
        RelationshipType relType
    ) {
        var propertyKeys = new ArrayList<>(graphStore.relationshipPropertyKeys(relType));

        var propertyConfigs = propertyKeys
            .stream()
            .map(key -> GraphFactory.PropertyConfig.of(Aggregation.NONE, DefaultValue.forDouble()))
            .collect(Collectors.toList());

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(outputNodes)
            .concurrency(1)
            .tracker(AllocationTracker.empty())
            .addAllPropertyConfigs(propertyConfigs)
            .build();

        var compositeIterator = graphStore.getCompositeRelationshipIterator(relType, propertyKeys);

        var propertyIndices = IntStream
            .range(0, propertyKeys.size())
            .boxed()
            .collect(Collectors.toMap(propertyKeys::get, idx -> idx));
        var relationshipContext = new EvaluationContext.RelationshipEvaluationContext(propertyIndices);

        outputNodes.forEachNode(node -> {
            var neoSource = outputNodes.toOriginalNodeId(node);

            compositeIterator.forEachRelationship(neoSource, (source, target, properties) -> {
                var neoTarget = inputNodes.toOriginalNodeId(target);
                var mappedTarget = outputNodes.toMappedNodeId(neoTarget);

                if (mappedTarget != NOT_FOUND) {
                    relationshipContext.init(relType.name, properties);

                    if (relationshipExpr.evaluate(relationshipContext) == Expression.TRUE) {
                        // TODO branching should happen somewhere else
                        if (properties.length == 0) {
                            relationshipsBuilder.add(neoSource, neoTarget);
                        } else if (properties.length == 1) {
                            relationshipsBuilder.add(neoSource, neoTarget, properties[0]);
                        } else {
                            relationshipsBuilder.add(neoSource, neoTarget, properties);
                        }
                    }
                }

                return true;
            });
            return true;
        });

        var relationships = relationshipsBuilder.buildAll();
        var topology = relationships.get(0).topology();
        var properties = IntStream.range(0, propertyKeys.size())
            .boxed()
            .collect(Collectors.toMap(
                propertyKeys::get,
                idx -> relationships.get(idx).properties().orElseThrow(IllegalStateException::new)
            ));

        return ImmutableFilteredRelationship.builder()
            .relationshipType(relType)
            .topology(topology)
            .properties(properties)
            .build();
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
                    return createNodePropertyStore(inputGraphStore, filteredNodeMapping, nodeLabel, propertyKeys);
                }
                )
            );
    }

    private static NodePropertyStore createNodePropertyStore(
        GraphStore inputGraphStore,
        NodeMapping filteredMapping,
        NodeLabel nodeLabel,
        Set<String> propertyKeys
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
