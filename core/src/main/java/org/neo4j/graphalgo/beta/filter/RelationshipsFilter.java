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

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.RelationshipProperty;
import org.neo4j.graphalgo.api.RelationshipPropertyStore;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.beta.filter.expression.EvaluationContext;
import org.neo4j.graphalgo.beta.filter.expression.Expression;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.values.storable.NumberType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.api.AdjacencyCursor.NOT_FOUND;

final class RelationshipsFilter {

    @ValueClass
    interface FilteredRelationships {

        Map<RelationshipType, Relationships.Topology> topology();

        Map<RelationshipType, RelationshipPropertyStore> propertyStores();
    }

    static FilteredRelationships filterRelationships(
        GraphStore graphStore,
        Expression expression,
        NodeMapping inputNodes,
        NodeMapping outputNodes,
        AllocationTracker allocationTracker
    ) {
        Map<RelationshipType, Relationships.Topology> topologies = new HashMap<>();
        Map<RelationshipType, RelationshipPropertyStore> relPropertyStores = new HashMap<>();

        for (RelationshipType relType : graphStore.relationshipTypes()) {
            var outputRelationships = filterRelationshipType(
                graphStore,
                expression,
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
                .tracker(allocationTracker)
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

    private RelationshipsFilter() {}
}
