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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.NodeFilteredGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.config.AlgoBaseConfig.ALL_NODE_LABELS;

public final class GraphStore {

    private final IdMap nodes;

    private final Map<String, NodeProperties> nodeProperties;

    private final Map<String, HugeGraph.TopologyCSR> relationships;

    private final Map<String, Map<String, HugeGraph.PropertyCSR>> relationshipProperties;

    private final Set<Graph> createdGraphs;

    private final AllocationTracker tracker;

    public static GraphStore of(
        IdMap nodes,
        Map<String, NodeProperties> nodeProperties,
        Map<String, HugeGraph.TopologyCSR> relationships,
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

        Map<String, HugeGraph.TopologyCSR> topology = singletonMap(relationshipType, relationships.topology());

        Map<String, NodeProperties> nodeProperties = graph.availableNodeProperties().stream()
            .collect(Collectors.toMap(property -> property, graph::nodeProperties));

        Map<String, Map<String, HugeGraph.PropertyCSR>> relationshipProperties = Collections.emptyMap();
        if (relationships.hasProperties() && relationshipProperty.isPresent()) {
            relationshipProperties = singletonMap(
                relationshipType,
                singletonMap(relationshipProperty.get(), relationships.properties().get())
            );
        }

        return GraphStore.of(graph.idMapping(), nodeProperties, topology, relationshipProperties, tracker);
    }

    private GraphStore(
        IdMap nodes,
        Map<String, NodeProperties> nodeProperties,
        Map<String, HugeGraph.TopologyCSR> relationships,
        Map<String, Map<String, HugeGraph.PropertyCSR>> relationshipProperties,
        AllocationTracker tracker
    ) {
        this.nodes = nodes;
        this.nodeProperties = new ConcurrentHashMap<>(nodeProperties);
        this.relationships = relationships;
        this.relationshipProperties = relationshipProperties;
        this.createdGraphs = new HashSet<>();
        this.tracker = tracker;
    }

    public IdMapping nodes() {
        return this.nodes;
    }

    public Set<String> nodeLabels() {
        return this
            .nodes
            .maybeLabelInformation
            .map(Map::keySet)
            .orElseGet(Collections::emptySet)
            .stream()
            .map(ElementIdentifier::name)
            .collect(Collectors.toSet());
    }

    public Set<String> nodePropertyKeys() {
        return nodeProperties.keySet();
    }

    public long nodePropertyCount() {
        return nodeProperties.size() * nodeCount();
    }

    public boolean hasNodeProperty(String propertyKey) {
        return nodeProperties.containsKey(propertyKey);
    }

    public void addNodeProperty(String propertyKey, NodeProperties nodeProperties) {
        this.nodeProperties.putIfAbsent(propertyKey, nodeProperties);
    }

    public NodeProperties nodeProperty(String propertyKey) {
        return this.nodeProperties.get(propertyKey);
    }

    public Set<String> relationshipTypes() {
        return relationships.keySet();
    }

    public boolean hasRelationshipType(String relationshipType) {
        return relationships.containsKey(relationshipType);
    }

    public long relationshipCount() {
        return relationships.values().stream()
            .mapToLong(HugeGraph.TopologyCSR::elementCount)
            .sum();
    }

    public long relationshipCount(String relationshipType) {
        return relationships.get(relationshipType).elementCount();
    }

    public long relationshipPropertyCount() {
        return relationshipProperties
            .values()
            .stream()
            .flatMapToLong(map -> map.values().stream().mapToLong(HugeGraph.PropertyCSR::elementCount))
            .sum();
    }

    public Set<String> relationshipPropertyKeys() {
        return relationshipProperties
            .values()
            .stream()
            .flatMap(properties -> properties.keySet().stream())
            .collect(Collectors.toSet());
    }

    public Set<String> relationshipPropertyKeys(String relationshipType) {
        return relationshipProperties.getOrDefault(relationshipType, Collections.emptyMap()).keySet();
    }

    public synchronized void addRelationshipType(String relationshipType, Optional<String> relationshipProperty, HugeGraph.Relationships relationships) {
        if (!hasRelationshipType(relationshipType)) {
            this.relationships.put(relationshipType, relationships.topology());

            if (relationshipProperty.isPresent() && relationships.hasProperties()) {
                HugeGraph.PropertyCSR propertyCSR = relationships.properties().get();
                this.relationshipProperties
                    .computeIfAbsent(relationshipType, ignore -> new HashMap<>())
                    .putIfAbsent(relationshipProperty.get(), propertyCSR);
            }
        }
    }

    public Graph getGraph(String... relationshipTypes) {
        return getGraph(ALL_NODE_LABELS, Arrays.asList(relationshipTypes), Optional.empty(), 1);
    }

    public Graph getGraph(String relationshipType, Optional<String> relationshipProperty) {
        return getGraph(ALL_NODE_LABELS, singletonList(relationshipType), relationshipProperty, 1);
    }

    public Graph getGraph(List<String> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        validateInput(relationshipTypes, maybeRelationshipProperty);
        return createGraph(ALL_NODE_LABELS, relationshipTypes, maybeRelationshipProperty, 1);
    }

    public Graph getGraph(List<String> nodeLabels, List<String> relationshipTypes, Optional<String> maybeRelationshipProperty, int concurrency) {
        validateInput(relationshipTypes, maybeRelationshipProperty);
        return createGraph(nodeLabels, relationshipTypes, maybeRelationshipProperty, concurrency);
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
                        .map(propertyKey -> createGraph(ALL_NODE_LABELS, relationshipType, Optional.of(propertyKey)));
                } else {
                    return Stream.of(createGraph(ALL_NODE_LABELS, relationshipType, Optional.empty()));
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

    private Graph createGraph(List<String> nodeLabels, String relationshipType, Optional<String> maybeRelationshipProperty) {
        return createGraph(nodeLabels, singletonList(relationshipType), maybeRelationshipProperty, 1);
    }

    private Graph createGraph(List<String> nodeLabels, List<String> relationshipTypes, Optional<String> maybeRelationshipProperty, int concurrency) {
        boolean loadAllRelationships = relationshipTypes.contains(PROJECT_ALL.name);
        boolean loadAllNodes = nodeLabels.contains(PROJECT_ALL.name);

        BitSet combinedBitSet = BitSet.newInstance();
        if (this.nodes.maybeLabelInformation.isPresent() && !loadAllNodes) {
            Map<ElementIdentifier, BitSet> labelInformation = this.nodes.maybeLabelInformation.get();
            validateNodeLabelFilter(nodeLabels, labelInformation);
            nodeLabels.forEach(label -> combinedBitSet.union(
                labelInformation.get(ElementIdentifier.of(label))));
        }

        boolean containsAllNodes = combinedBitSet.cardinality() == this.nodes.nodeCount();

        Optional<IdMap> filteredNodes = loadAllNodes || !this.nodes.maybeLabelInformation.isPresent() || containsAllNodes
            ? Optional.empty()
            : Optional.of(this.nodes.withFilteredLabels(combinedBitSet, concurrency));

        List<Graph> filteredGraphs = relationships.entrySet().stream()
            .filter(relTypeAndCSR -> loadAllRelationships || relationshipTypes.contains(relTypeAndCSR.getKey()))
            .map(relTypeAndCSR -> {
                HugeGraph initialGraph = HugeGraph.create(
                    this.nodes,
                    nodeProperties,
                    relTypeAndCSR.getValue(),
                    maybeRelationshipProperty.map(propertyKey -> relationshipProperties
                        .get(relTypeAndCSR.getKey())
                        .get(propertyKey)),
                    tracker
                );

                if (filteredNodes.isPresent()) {
                    return new NodeFilteredGraph(initialGraph, filteredNodes.get());
                } else {
                    return initialGraph;
                }
            })
            .collect(Collectors.toList());

        filteredGraphs.forEach(graph -> graph.canRelease(false));
        createdGraphs.addAll(filteredGraphs);
        return UnionGraph.of(filteredGraphs);
    }

    private void validateNodeLabelFilter(List<String> nodeLabels, Map<ElementIdentifier, BitSet> labelInformation) {
        List<String> invalidLabels = nodeLabels
            .stream()
            .filter(label -> !labelInformation
                .keySet()
                .stream()
                .map(ElementIdentifier::name)
                .collect(Collectors.toSet())
                .contains(label))
            .collect(Collectors.toList());
        if (!invalidLabels.isEmpty()) {
            throw new IllegalArgumentException(String.format("Specified labels %s do not correspond to any of the node projections %s.",invalidLabels, labelInformation.keySet()));
        }
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

