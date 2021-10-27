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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.BitSet;
import org.immutables.builder.Builder.AccessibleFields;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapGraph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.UnionNodeProperties;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.NodeFilteredGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.values.storable.NumberType;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.graphalgo.NodeLabel.ALL_NODES;
import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.graphalgo.config.AlgoBaseConfig.ALL_NODE_LABEL_IDENTIFIERS;

public class GraphStore {

    private final IdMap nodes;

    private final Map<NodeLabel, NodePropertyStore> nodeProperties;

    private final Map<RelationshipType, HugeGraph.TopologyCSR> relationships;

    private final Map<RelationshipType, RelationshipPropertyStore> relationshipProperties;

    private final Set<Graph> createdGraphs;

    private final AllocationTracker tracker;

    private LocalDateTime modificationTime;

    public static GraphStore of(
        IdMap nodes,
        Map<NodeLabel, Map<String, NodeProperties>> nodeProperties,
        Map<RelationshipType, HugeGraph.TopologyCSR> relationships,
        Map<RelationshipType, Map<String, HugeGraph.PropertyCSR>> relationshipProperties,
        AllocationTracker tracker
    ) {
        Map<NodeLabel, NodePropertyStore> nodePropertyStores = new HashMap<>(nodeProperties.size());
        nodeProperties.forEach((nodeLabel, propertyMap) -> {
            NodePropertyStore.Builder builder = NodePropertyStore.builder();
            propertyMap.forEach((propertyKey, propertyValues) -> builder.putNodeProperty(
                propertyKey,
                NodeProperty.of(propertyKey, NumberType.FLOATING_POINT, PropertyState.PERSISTENT, propertyValues)
            ));
            nodePropertyStores.put(nodeLabel, builder.build());
        });

        Map<RelationshipType, RelationshipPropertyStore> relationshipPropertyStores = new HashMap<>();
        relationshipProperties.forEach((relationshipType, propertyMap) -> {
            RelationshipPropertyStore.Builder builder = RelationshipPropertyStore.builder();
            propertyMap.forEach((propertyKey, propertyValues) -> builder.putRelationshipProperty(
                propertyKey,
                RelationshipProperty.of(propertyKey, NumberType.FLOATING_POINT, PropertyState.PERSISTENT, propertyValues)
            ));
            relationshipPropertyStores.put(relationshipType, builder.build());
        });

        return new GraphStore(
            nodes,
            nodePropertyStores,
            relationships,
            relationshipPropertyStores,
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

        Map<RelationshipType, HugeGraph.TopologyCSR> topology = singletonMap(RelationshipType.of(relationshipType), relationships.topology());

        Map<NodeLabel, Map<String, NodeProperties>> nodeProperties = new HashMap<>();
        nodeProperties.put(
            ALL_NODES,
            graph.availableNodeProperties().stream().collect(toMap(
                Function.identity(),
                graph::nodeProperties
            ))
        );

        Map<RelationshipType, Map<String, HugeGraph.PropertyCSR>> relationshipProperties = Collections.emptyMap();
        if (relationshipProperty.isPresent() && relationships.properties().isPresent()) {
            relationshipProperties = singletonMap(
                RelationshipType.of(relationshipType),
                singletonMap(relationshipProperty.get(), relationships.properties().get())
            );
        }

        return GraphStore.of(graph.idMap(), nodeProperties, topology, relationshipProperties, tracker);
    }

    protected GraphStore(
        IdMap nodes,
        Map<NodeLabel, NodePropertyStore> nodeProperties,
        Map<RelationshipType, HugeGraph.TopologyCSR> relationships,
        Map<RelationshipType, RelationshipPropertyStore> relationshipProperties,
        AllocationTracker tracker
    ) {
        this.nodes = nodes;
        this.nodeProperties = nodeProperties;
        this.relationships = relationships;
        this.relationshipProperties = relationshipProperties;
        this.createdGraphs = new HashSet<>();
        this.modificationTime = LocalDateTime.now();
        this.tracker = tracker;
    }

    public LocalDateTime modificationTime() {
        return modificationTime;
    }

    public IdMap nodes() {
        return this.nodes;
    }

    public Set<NodeLabel> nodeLabels() {
        return new HashSet<>(this
            .nodes
            .maybeLabelInformation
            .map(Map::keySet)
            .orElseGet(() -> Collections.singleton(ALL_NODES)));
    }

    public Set<String> nodePropertyKeys(NodeLabel label) {
        return new HashSet<>(nodeProperties.getOrDefault(label, NodePropertyStore.empty()).keySet());
    }

    public Map<ElementIdentifier, Set<String>> nodePropertyKeys() {
        return nodeLabels().stream().collect(Collectors.toMap(Function.identity(), this::nodePropertyKeys));
    }

    public long nodePropertyCount() {
        // TODO: This is not the correct value. We would need to look into the bitsets in order to retrieve the correct value.
        return nodeProperties.values().stream()
                   .mapToLong(nodePropertyStore -> nodePropertyStore.keySet().size())
                   .sum() * nodeCount();
    }

    public boolean hasNodeProperty(Collection<NodeLabel> labels, String propertyKey) {
        return labels
            .stream()
            .allMatch(label -> nodeProperties.containsKey(label) && nodeProperties.get(label).containsKey(propertyKey));
    }

    public void addNodeProperty(NodeLabel nodeLabel, String propertyKey, NumberType propertyType, NodeProperties propertyValues) {
        updateGraphStore((graphStore) -> graphStore.nodeProperties.compute(nodeLabel, (k, nodePropertyStore) -> {
            NodePropertyStore.Builder storeBuilder = NodePropertyStore.builder();
            if (nodePropertyStore != null) {
                storeBuilder.from(nodePropertyStore);
            }
            return storeBuilder
                .putIfAbsent(propertyKey, NodeProperty.of(propertyKey, propertyType, PropertyState.TRANSIENT, propertyValues))
                .build();
        }));
    }

    public void removeNodeProperty(NodeLabel nodeLabel, String propertyKey) {
        updateGraphStore(graphStore -> {
            if (graphStore.nodeProperties.containsKey(nodeLabel)) {
                NodePropertyStore updatedNodePropertyStore = NodePropertyStore.builder()
                    .from(graphStore.nodeProperties.get(nodeLabel))
                    .removeProperty(propertyKey)
                    .build();

                if (updatedNodePropertyStore.isEmpty()) {
                    graphStore.nodeProperties.remove(nodeLabel);
                } else {
                    graphStore.nodeProperties.replace(nodeLabel, updatedNodePropertyStore);
                }
            }
        });
    }

    public NodeProperty nodeProperty(String propertyKey) {
        if (nodes.maybeLabelInformation.isPresent()) {
            Map<NodeLabel, NodeProperties> unionValues = new HashMap<>();
            NumberType unionType = NumberType.NO_NUMBER;
            PropertyState unionOrigin = PropertyState.PERSISTENT;

            for (Map.Entry<NodeLabel, NodePropertyStore> labelAndPropertyStore : nodeProperties.entrySet()) {
                NodeLabel nodeLabel = labelAndPropertyStore.getKey();
                NodePropertyStore nodePropertyStore = labelAndPropertyStore.getValue();
                if (nodePropertyStore.containsKey(propertyKey)) {
                    NodeProperty nodeProperty = nodePropertyStore.get(propertyKey);
                    unionValues.put(nodeLabel, nodeProperty.values());
                    unionType = nodeProperty.type();
                    unionOrigin = nodeProperty.state();
                }
            }

            return NodeProperty.of(
                propertyKey,
                unionType,
                unionOrigin,
                new UnionNodeProperties(unionValues, nodes.maybeLabelInformation.get())
            );
        }
        return nodeProperties.get(ALL_NODES).get(propertyKey);
    }

    public NumberType nodePropertyType(String propertyKey) {
        return nodeProperties.values().stream()
            .filter(propertyStore -> propertyStore.containsKey(propertyKey))
            .map(propertyStore -> propertyStore.get(propertyKey).type())
            .findFirst()
            .orElse(NumberType.NO_NUMBER);
    }

    public NodeProperty nodeProperty(NodeLabel label, String propertyKey) {
        return this.nodeProperties.getOrDefault(label, NodePropertyStore.empty()).get(propertyKey);
    }

    public Set<RelationshipType> relationshipTypes() {
        return relationships.keySet();
    }

    public boolean hasRelationshipType(RelationshipType relationshipType) {
        return relationships.containsKey(relationshipType);
    }

    public long relationshipCount() {
        return relationships.values().stream()
            .mapToLong(HugeGraph.TopologyCSR::elementCount)
            .sum();
    }

    public long relationshipCount(RelationshipType relationshipType) {
        return relationships.get(relationshipType).elementCount();
    }

    public NumberType relationshipPropertyType(String propertyKey) {
        return relationshipProperties.values().stream()
            .filter(propertyStore -> propertyStore.containsKey(propertyKey))
            .map(propertyStore -> propertyStore.get(propertyKey).type())
            .findFirst()
            .orElse(NumberType.NO_NUMBER);
    }

    public long relationshipPropertyCount() {
        return relationshipProperties
            .values()
            .stream()
            .flatMapToLong(relationshipPropertyStore -> relationshipPropertyStore
                .values()
                .stream()
                .map(RelationshipProperty::values)
                .mapToLong(HugeGraph.PropertyCSR::elementCount))
            .sum();
    }

    public Set<String> relationshipPropertyKeys() {
        return relationshipProperties
            .values()
            .stream()
            .flatMap(relationshipPropertyStore -> relationshipPropertyStore.keySet().stream())
            .collect(Collectors.toSet());
    }

    public Set<String> relationshipPropertyKeys(RelationshipType relationshipType) {
        return relationshipProperties.getOrDefault(relationshipType, RelationshipPropertyStore.empty()).keySet();
    }

    public void addRelationshipType(
        RelationshipType relationshipType,
        Optional<String> relationshipPropertyKey,
        Optional<NumberType> relationshipPropertyType,
        HugeGraph.Relationships relationships
    ) {
        updateGraphStore(graphStore -> {
            if (!hasRelationshipType(relationshipType)) {
                graphStore.relationships.put(relationshipType, relationships.topology());

                if (relationshipPropertyKey.isPresent()
                    && relationshipPropertyType.isPresent()
                    && relationships.properties().isPresent()) {
                    addRelationshipProperty(
                        relationshipType,
                        relationshipPropertyKey.get(),
                        relationshipPropertyType.get(),
                        relationships.properties().get(),
                        graphStore
                    );
                }
            }
        });
    }

    private void addRelationshipProperty(
        RelationshipType relationshipType,
        String propertyKey,
        NumberType propertyType,
        HugeGraph.PropertyCSR propertyCSR,
        GraphStore graphStore
    ) {
        graphStore.relationshipProperties.compute(relationshipType, (relType, propertyStore) -> {
            RelationshipPropertyStore.Builder builder = RelationshipPropertyStore.builder();
            if (propertyStore != null) {
                 builder.from(propertyStore);
            }
            return builder.putIfAbsent(
                propertyKey,
                ImmutableRelationshipProperty.of(propertyKey, propertyType, PropertyState.TRANSIENT, propertyCSR)
            ).build();
        });
    }

    public DeletionResult deleteRelationships(RelationshipType relationshipType) {
        return DeletionResult.of(builder ->
            updateGraphStore(graphStore -> {
                builder.deletedRelationships(graphStore.relationships.get(relationshipType).elementCount());
                graphStore.relationshipProperties
                    .getOrDefault(relationshipType, RelationshipPropertyStore.empty())
                    .relationshipProperties().values().forEach(property -> {
                    builder.putDeletedProperty(property.key(), property.values().elementCount());
                });
                graphStore.relationships.remove(relationshipType);
                graphStore.relationshipProperties.remove(relationshipType);
            })
        );
    }

    public Graph getGraph(RelationshipType... relationshipTypes) {
        return getGraph(ALL_NODE_LABEL_IDENTIFIERS, Arrays.asList(relationshipTypes), Optional.empty(), 1);
    }

    public Graph getGraph(RelationshipType relationshipType, Optional<String> relationshipProperty) {
        return getGraph(ALL_NODE_LABEL_IDENTIFIERS, singletonList(relationshipType), relationshipProperty, 1);
    }

    public Graph getGraph(List<RelationshipType> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        validateInput(relationshipTypes, maybeRelationshipProperty);
        return createGraph(ALL_NODE_LABEL_IDENTIFIERS, relationshipTypes, maybeRelationshipProperty, 1);
    }

    public Graph getGraph(
        List<NodeLabel> nodeLabels,
        List<RelationshipType> relationshipTypes,
        Optional<String> maybeRelationshipProperty,
        int concurrency
    ) {
        validateInput(relationshipTypes, maybeRelationshipProperty);
        return createGraph(nodeLabels, relationshipTypes, maybeRelationshipProperty, concurrency);
    }

    public IdMapGraph getUnion() {
        return UnionGraph.of(relationships
            .keySet()
            .stream()
            .flatMap(relationshipType -> {
                if (relationshipProperties.containsKey(relationshipType)) {
                    return relationshipProperties
                        .get(relationshipType)
                        .keySet()
                        .stream()
                        .map(propertyKey -> createGraph(ALL_NODE_LABEL_IDENTIFIERS, relationshipType, Optional.of(propertyKey)));
                } else {
                    return Stream.of(createGraph(ALL_NODE_LABEL_IDENTIFIERS, relationshipType, Optional.empty()));
                }
            })
            .collect(Collectors.toList()));
    }

    public void canRelease(boolean canRelease) {
        createdGraphs.forEach(graph -> graph.canRelease(canRelease));
    }

    public void release() {
        createdGraphs.forEach(Graph::release);
    }

    public long nodeCount() {
        return nodes.nodeCount();
    }

    private IdMapGraph createGraph(
        List<NodeLabel> nodeLabels,
        RelationshipType relationshipType,
        Optional<String> maybeRelationshipProperty
    ) {
        return createGraph(nodeLabels, singletonList(relationshipType), maybeRelationshipProperty, 1);
    }

    protected IdMapGraph createGraph(
        List<NodeLabel> filteredLabels,
        List<RelationshipType> relationshipTypes,
        Optional<String> maybeRelationshipProperty,
        int concurrency
    ) {
        boolean loadAllRelationships = relationshipTypes.contains(ALL_RELATIONSHIPS);
        boolean loadAllNodes = filteredLabels.contains(ALL_NODES);

        Collection<NodeLabel> expandedLabels = loadAllNodes ? nodeLabels() : filteredLabels;

        boolean containsAllNodes = true;
        BitSet combinedBitSet = BitSet.newInstance();

        if (this.nodes.maybeLabelInformation.isPresent() && !loadAllNodes) {
            Map<NodeLabel, BitSet> labelInformation = this.nodes.maybeLabelInformation.get();
            validateNodeLabelFilter(expandedLabels, labelInformation);
            expandedLabels.forEach(label -> combinedBitSet.union(labelInformation.get(label)));
            containsAllNodes = combinedBitSet.cardinality() == this.nodes.nodeCount();
        }

        Optional<IdMap> filteredNodes = loadAllNodes || !this.nodes.maybeLabelInformation.isPresent() || containsAllNodes
            ? Optional.empty()
            : Optional.of(this.nodes.withFilteredLabels(combinedBitSet, concurrency));

        List<IdMapGraph> filteredGraphs = relationships.entrySet().stream()
            .filter(relTypeAndCSR -> loadAllRelationships || relationshipTypes.contains(relTypeAndCSR.getKey()))
            .map(relTypeAndCSR -> {
                Map<String, NodeProperties> filteredNodeProperties = filterNodeProperties(expandedLabels, nodes.maybeLabelInformation);

                HugeGraph initialGraph = HugeGraph.create(
                    this.nodes,
                    filteredNodeProperties,
                    relTypeAndCSR.getValue(),
                    maybeRelationshipProperty.map(propertyKey -> relationshipProperties
                        .get(relTypeAndCSR.getKey())
                        .get(propertyKey).values()),
                    tracker
                );

                if (filteredNodes.isPresent()) {
                    return new NodeFilteredGraph(initialGraph, filteredNodes.get(), tracker);
                } else {
                    return initialGraph;
                }
            })
            .collect(Collectors.toList());

        filteredGraphs.forEach(graph -> graph.canRelease(false));
        createdGraphs.addAll(filteredGraphs);
        return UnionGraph.of(filteredGraphs);
    }

    private Map<String, NodeProperties> filterNodeProperties(
        Collection<NodeLabel> labels,
        Optional<Map<NodeLabel, BitSet>> maybeElementIdentifierBitSetMap
    ) {
        if (this.nodeProperties.isEmpty()) {
            return Collections.emptyMap();
        }
        if (labels.size() == 1 || !maybeElementIdentifierBitSetMap.isPresent()) {
            return this.nodeProperties.get(labels.iterator().next()).nodePropertyValues();
        }

        Map<String, Map<NodeLabel, NodeProperties>> invertedNodeProperties = new HashMap<>();
        nodeProperties
            .entrySet()
            .stream()
            .filter(entry -> labels.contains(entry.getKey()) || labels.contains(ALL_NODES))
            .forEach(entry -> entry.getValue().nodeProperties()
                .forEach((propertyKey, nodeProperty) -> invertedNodeProperties
                    .computeIfAbsent(
                        propertyKey,
                        ignored -> new HashMap<>()
                    )
                    .put(entry.getKey(), nodeProperty.values())
                ));

        return invertedNodeProperties
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new UnionNodeProperties(entry.getValue(), maybeElementIdentifierBitSetMap.get())
            ));
    }

    private void validateNodeLabelFilter(Collection<NodeLabel> nodeLabels, Map<NodeLabel, BitSet> labelInformation) {
        List<ElementIdentifier> invalidLabels = nodeLabels
            .stream()
            .filter(label -> !new HashSet<>(labelInformation.keySet()).contains(label))
            .collect(Collectors.toList());
        if (!invalidLabels.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "Specified labels %s do not correspond to any of the node projections %s.",
                invalidLabels,
                labelInformation.keySet()
            ));
        }
    }

    private void validateInput(Collection<RelationshipType> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        if (relationshipTypes.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "The parameter '%s' should not be empty. Use '*' to load all relationship types.",
                ProcedureConstants.RELATIONSHIP_TYPES
            ));
        }

        if (!relationshipTypes.contains(ALL_RELATIONSHIPS)) {
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

    private synchronized void updateGraphStore(Consumer<GraphStore> updateFunction) {
        updateFunction.accept(this);
        this.modificationTime = LocalDateTime.now();
    }

    public enum PropertyState {
        PERSISTENT, TRANSIENT
    }

    @ValueClass
    public interface NodePropertyStore {

        Map<String, NodeProperty> nodeProperties();

        default Map<String, NodeProperties> nodePropertyValues() {
            return nodeProperties()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().values()));
        }

        default NodeProperty get(String propertyKey) {
            return nodeProperties().get(propertyKey);
        }

        default boolean isEmpty() {
            return nodeProperties().isEmpty();
        }

        default Set<String> keySet() {
            return nodeProperties().keySet();
        }

        default boolean containsKey(String propertyKey) {
            return nodeProperties().containsKey(propertyKey);
        }

        static NodePropertyStore empty() {
            return ImmutableNodePropertyStore.of(Collections.emptyMap());
        }

        static Builder builder() {
            // need to initialize with empty map due to `deferCollectionAllocation = true`
            return new Builder().nodeProperties(Collections.emptyMap());
        }

        @AccessibleFields
        final class Builder extends ImmutableNodePropertyStore.Builder {

            Builder putIfAbsent(String propertyKey, NodeProperty nodeProperty) {
                nodeProperties.putIfAbsent(propertyKey, nodeProperty);
                return this;
            }

            Builder removeProperty(String propertyKey) {
                nodeProperties.remove(propertyKey);
                return this;
            }
        }
    }

    @ValueClass
    public interface NodeProperty {

        String key();

        NumberType type();

        PropertyState state();

        NodeProperties values();

        static NodeProperty of(String key, NumberType type, PropertyState origin, NodeProperties values) {
            return ImmutableNodeProperty.of(key, type, origin, values);
        }
    }

    @ValueClass
    public interface RelationshipPropertyStore {

        Map<String, RelationshipProperty> relationshipProperties();

        default RelationshipProperty get(String propertyKey) {
            return relationshipProperties().get(propertyKey);
        }

        default boolean isEmpty() {
            return relationshipProperties().isEmpty();
        }

        default Set<String> keySet() {
            return relationshipProperties().keySet();
        }

        default Collection<RelationshipProperty> values() {
            return relationshipProperties().values();
        }

        default boolean containsKey(String propertyKey) {
            return relationshipProperties().containsKey(propertyKey);
        }

        static RelationshipPropertyStore empty() {
            return ImmutableRelationshipPropertyStore.of(Collections.emptyMap());
        }

        static Builder builder() {
            // need to initialize with empty map due to `deferCollectionAllocation = true`
            return new Builder().relationshipProperties(Collections.emptyMap());
        }

        @AccessibleFields
        final class Builder extends ImmutableRelationshipPropertyStore.Builder {

            Builder putIfAbsent(String propertyKey, RelationshipProperty relationshipProperty) {
                relationshipProperties.putIfAbsent(propertyKey, relationshipProperty);
                return this;
            }
        }

    }

    @ValueClass
    public interface RelationshipProperty {

        String key();

        NumberType type();

        PropertyState state();

        HugeGraph.PropertyCSR values();

        static RelationshipProperty of(String key, NumberType type, PropertyState state, HugeGraph.PropertyCSR values) {
            return ImmutableRelationshipProperty.of(key, type, state, values);
        }
    }
}



