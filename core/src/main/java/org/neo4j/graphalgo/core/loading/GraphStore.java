/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;

public final class GraphStore {

    private final IdMap nodes;

    private final Map<String, NodeProperties> nodeProperties;

    private final Map<String, HugeGraph.CSR> relationships;

    private final Map<String, Map<String, HugeGraph.PropertyCSR>> relationshipProperties;

    private final Set<Graph> createdGraphs;

    private final AllocationTracker tracker;

    public static GraphStore of(
        IdMap nodes,
        Map<String, NodeProperties> nodeProperties,
        Map<String, HugeGraph.CSR> relationships,
        Map<String, Map<String, HugeGraph.PropertyCSR>> relationshipProperties,
        AllocationTracker tracker
    ) {
        return new GraphStore(
            nodes,
            nodeProperties,
            relationships,
            relationshipProperties,
            tracker
        );
    }

    public static GraphStore of(
        HugeGraph graph,
        String relationshipType,
        Optional<String> relationshipProperty,
        AllocationTracker tracker
    ) {
        HugeGraph.Relationships relationships = graph.relationships();

        Map<String, HugeGraph.CSR> topology = singletonMap(relationshipType, relationships.topology());

        Map<String, NodeProperties> nodeProperties = graph.availableNodeProperties().stream()
            .collect(Collectors.toMap(property -> property, graph::nodeProperties));

        Map<String, Map<String, HugeGraph.PropertyCSR>> relationshipProperties = Collections.emptyMap();
        if (relationshipProperty.isPresent() && graph.hasRelationshipProperty()) {
            relationshipProperties = singletonMap(
                relationshipType,
                singletonMap(relationshipProperty.get(), relationships.properties())
            );
        }

        return GraphStore.of(graph.idMapping(), nodeProperties, topology, relationshipProperties, tracker);
    }

    private GraphStore(
        IdMap nodes,
        Map<String, NodeProperties> nodeProperties,
        Map<String, HugeGraph.CSR> relationships,
        Map<String, Map<String, HugeGraph.PropertyCSR>> relationshipProperties,
        AllocationTracker tracker
    ) {
        this.nodes = nodes;
        this.nodeProperties = nodeProperties;
        this.relationships = relationships;
        this.relationshipProperties = relationshipProperties;
        this.createdGraphs = new HashSet<>();
        this.tracker = tracker;
    }

    public Graph getGraph(String... relationshipTypes) {
        return getGraph(Arrays.asList(relationshipTypes), Optional.empty());
    }

    public Graph getGraph(String relationshipType, Optional<String> relationshipProperty) {
        return getGraph(singletonList(relationshipType), relationshipProperty);
    }

    public Graph getGraph(List<String> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        validateInput(relationshipTypes, maybeRelationshipProperty);
        return createGraph(relationshipTypes, maybeRelationshipProperty);
    }

    public Graph getUnion() {
        return UnionGraph.of(relationships
            .keySet()
            .stream()
            .flatMap(relationshipType -> {
                if (relationshipProperties.containsKey(relationshipType)) {
                    return relationshipProperties
                        .get(relationshipType)
                        .keySet()
                        .stream()
                        .map(propertyKey -> createGraph(relationshipType, Optional.of(propertyKey)));
                } else {
                    return Stream.of(createGraph(relationshipType, Optional.empty()));
                }
            })
            .collect(Collectors.toList()));
    }

    public void canRelease(boolean canRelease) {
        createdGraphs.forEach(graph -> graph.canRelease(canRelease));
    }

    public long nodeCount() {
        return nodes.nodeCount();
    }

    public long relationshipCount() {
        return relationships.values().stream()
            .mapToLong(HugeGraph.CSR::elementCount)
            .sum();
    }

    public Set<String> relationshipTypes() {
        return relationships.keySet();
    }

    private Graph createGraph(String relationshipType, Optional<String> maybeRelationshipProperty) {
        return createGraph(singletonList(relationshipType), maybeRelationshipProperty);
    }

    private Graph createGraph(List<String> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        boolean loadAllRelationships = relationshipTypes.contains(PROJECT_ALL.name);

        List<Graph> filteredGraphs = relationships.entrySet().stream()
            .filter(relTypeAndCSR -> loadAllRelationships || relationshipTypes.contains(relTypeAndCSR.getKey()))
            .map(relTypeAndCSR -> HugeGraph.create(
                nodes, nodeProperties, relTypeAndCSR.getValue(), maybeRelationshipProperty.map(propertyKey -> relationshipProperties
                    .get(relTypeAndCSR.getKey())
                    .get(propertyKey)), tracker
            ))
            .collect(Collectors.toList());

        filteredGraphs.forEach(graph -> graph.canRelease(false));
        createdGraphs.addAll(filteredGraphs);
        return UnionGraph.of(filteredGraphs);
    }

    private void validateInput(Collection<String> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        if (relationshipTypes.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "The parameter '%s' should not be empty. Use '*' to load all relationship types.",
                ProcedureConstants.RELATIONSHIP_TYPES
            ));
        }

        if (!relationshipTypes.contains(PROJECT_ALL.name)) {
            relationshipTypes.forEach(relationshipType -> {
                if (!relationships.containsKey(relationshipType)) {
                    throw new IllegalArgumentException(String.format(
                        "No relationships have been loaded for relationship type '%s'",
                        relationshipType
                    ));
                }

                maybeRelationshipProperty.ifPresent(relationshipProperty -> {
                    if (!relationshipProperties.get(relationshipType).containsKey(relationshipProperty)) {
                        throw new IllegalArgumentException(String.format(
                            "No relationships have been loaded for relationship type '%s' and relationship property '%s'.",
                            relationshipType,
                            maybeRelationshipProperty.get()
                        ));
                    }
                });
            });
        }
    }
}

