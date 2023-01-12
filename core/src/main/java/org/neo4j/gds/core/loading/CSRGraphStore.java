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
package org.neo4j.gds.core.loading;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphCharacteristics;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.Properties;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Topology;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.graph.GraphProperty;
import org.neo4j.gds.api.properties.graph.GraphPropertyStore;
import org.neo4j.gds.api.properties.graph.GraphPropertyValues;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.core.huge.CSRCompositeRelationshipIterator;
import org.neo4j.gds.core.huge.HugeGraphBuilder;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.core.huge.UnionGraph;
import org.neo4j.gds.core.utils.TimeUtil;
import org.neo4j.gds.utils.ExceptionUtil;
import org.neo4j.gds.utils.StringJoining;

import java.time.ZonedDateTime;
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

import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.core.StringSimilarity.prettySuggestions;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Value.Style(typeBuilder = "GraphStoreBuilder")
public class CSRGraphStore implements GraphStore {

    private final int concurrency;

    private final DatabaseId databaseId;

    private final Capabilities capabilities;

    private final IdMap nodes;

    private final Map<RelationshipType, SingleTypeRelationships> relationships;

    private final Set<Graph> createdGraphs;

    private GraphSchema schema;

    private GraphPropertyStore graphProperties;

    private NodePropertyStore nodeProperties;

    private ZonedDateTime modificationTime;

    @Builder.Factory
    public static CSRGraphStore of(
        DatabaseId databaseId,
        Capabilities capabilities,
        GraphSchema schema,
        Nodes nodes,
        RelationshipImportResult relationshipImportResult,
        Optional<GraphPropertyStore> graphProperties,
        int concurrency
    ) {
        return new CSRGraphStore(
            databaseId,
            capabilities,
            schema,
            nodes.idMap(),
            nodes.properties(),
            relationshipImportResult.importResults(),
            graphProperties.orElseGet(GraphPropertyStore::empty),
            concurrency
        );
    }

    private CSRGraphStore(
        DatabaseId databaseId,
        Capabilities capabilities,
        GraphSchema schema,
        IdMap nodes,
        NodePropertyStore nodeProperties,
        Map<RelationshipType, SingleTypeRelationships> relationships,
        GraphPropertyStore graphProperties,
        int concurrency
    ) {
        this.databaseId = databaseId;
        this.capabilities = capabilities;

        this.schema = schema;

        this.graphProperties = graphProperties;

        this.nodes = nodes;
        this.nodeProperties = nodeProperties;

        // We want mutable collections inside the GraphStore
        this.relationships = new HashMap<>(relationships);

        this.concurrency = concurrency;
        this.createdGraphs = new HashSet<>();
        this.modificationTime = TimeUtil.now();
    }

    @Override
    public DatabaseId databaseId() {
        return databaseId;
    }

    @Override
    public GraphSchema schema() {
        return schema;
    }

    @Override
    public ZonedDateTime modificationTime() {
        return modificationTime;
    }

    @Override
    public Capabilities capabilities() {
        return capabilities;
    }

    // Graph properties

    @Override
    public Set<String> graphPropertyKeys() {
        return graphProperties.keySet();
    }

    @Override
    public boolean hasGraphProperty(String propertyKey) {
        return graphPropertyKeys().contains(propertyKey);
    }

    @Override
    public GraphProperty graphProperty(String propertyKey) {
        return graphProperties.get(propertyKey);
    }

    @Override
    public ValueType graphPropertyType(String propertyKey) {
        return graphProperty(propertyKey).valueType();
    }

    @Override
    public GraphPropertyValues graphPropertyValues(String propertyKey) {
        return graphProperty(propertyKey).values();
    }

    @Override
    public void addGraphProperty(String propertyKey, GraphPropertyValues propertyValues) {
        updateGraphStore((graphStore) -> {
            if (graphStore.hasGraphProperty(propertyKey)) {
                throw new UnsupportedOperationException(formatWithLocale("Graph property %s already exists",
                    propertyKey
                ));
            }

            graphStore.graphProperties = GraphPropertyStore
                .builder()
                .from(graphStore.graphProperties)
                .putIfAbsent(propertyKey, GraphProperty.of(propertyKey, propertyValues))
                .build();

            var newGraphPropertySchema = new HashMap<>(schema().graphProperties());
            newGraphPropertySchema.put(propertyKey, PropertySchema.of(propertyKey, propertyValues.valueType()));

            this.schema = GraphSchema.of(schema().nodeSchema(), schema().relationshipSchema(), newGraphPropertySchema);
        });
    }

    @Override
    public void removeGraphProperty(String propertyKey) {
        updateGraphStore(graphStore -> {
            graphStore.graphProperties = GraphPropertyStore
                .builder()
                .from(graphStore.graphProperties)
                .removeProperty(propertyKey)
                .build();

            var newGraphPropertySchema = new HashMap<>(schema().graphProperties());
            newGraphPropertySchema.remove(propertyKey);

            this.schema = GraphSchema.of(schema().nodeSchema(), schema().relationshipSchema(), newGraphPropertySchema);
        });
    }

    @Override
    public IdMap nodes() {
        return this.nodes;
    }

    @Override
    public Set<NodeLabel> nodeLabels() {
        assert schema().nodeSchema().availableLabels().size() == nodes.availableNodeLabels().size();
        return nodes.availableNodeLabels();
    }

    @Override
    public void addNodeLabel(NodeLabel nodeLabel) {
        updateGraphStore(graphStore -> {
            nodes.addNodeLabel(nodeLabel);
            var nodeSchema = schema.nodeSchema();
            nodeSchema.addLabel(nodeLabel);
            nodeSchema.copyUnionPropertiesToLabel(nodeLabel);
        });
    }

    @Override
    public Set<String> nodePropertyKeys(NodeLabel label) {
        return schema().nodeSchema().allProperties(label);
    }

    @Override
    public Set<String> nodePropertyKeys() {
        assert schema().nodeSchema().allProperties().size() == nodeProperties.keySet().size();
        return nodeProperties.keySet();
    }

    @Override
    public boolean hasNodeProperty(String propertyKey) {
        return nodeProperties.containsKey(propertyKey);
    }

    @Override
    public boolean hasNodeProperty(NodeLabel label, String propertyKey) {
        return schema().nodeSchema().hasProperty(label, propertyKey) && hasNodeProperty(propertyKey);
    }

    @Override
    public boolean hasNodeProperty(Collection<NodeLabel> labels, String propertyKey) {
        return labels.stream().allMatch(label -> hasNodeProperty(label, propertyKey));
    }

    @Override
    public void addNodeProperty(
        Set<NodeLabel> labels, String propertyKey, NodePropertyValues propertyValues
    ) {
        updateGraphStore((graphStore) -> {
            if (graphStore.hasNodeProperty(propertyKey)) {
                throw new UnsupportedOperationException(formatWithLocale("Node property %s already exists",
                    propertyKey
                ));
            }

            graphStore.nodeProperties = NodePropertyStore
                .builder()
                .from(graphStore.nodeProperties)
                .putIfAbsent(propertyKey, NodeProperty.of(propertyKey, PropertyState.TRANSIENT, propertyValues))
                .build();


            labels.forEach(label -> schema().nodeSchema().get(label).addProperty(propertyKey, PropertySchema.of(
                propertyKey,
                propertyValues.valueType(),
                propertyValues.valueType().fallbackValue(),
                PropertyState.TRANSIENT
            )));
        });
    }

    @Override
    public void removeNodeProperty(String propertyKey) {
        updateGraphStore(graphStore -> {
            graphStore.nodeProperties = NodePropertyStore
                .builder()
                .from(graphStore.nodeProperties)
                .removeProperty(propertyKey)
                .build();

            schema().nodeSchema().entries().forEach(entry -> entry.removeProperty(propertyKey));
        });
    }

    @Override
    public NodeProperty nodeProperty(String propertyKey) {
        return this.nodeProperties.get(propertyKey);
    }

    @Override
    public Set<RelationshipType> relationshipTypes() {
        return relationships.keySet();
    }

    @Override
    public boolean hasRelationshipType(RelationshipType relationshipType) {
        return relationships.containsKey(relationshipType);
    }

    @Override
    public long relationshipCount() {
        long sum = 0L;
        for (var relationship : relationships.values()) {
            long elementCount = relationship.topology().elementCount();
            sum += elementCount;
        }
        return sum;
    }

    @Override
    public long relationshipCount(RelationshipType relationshipType) {
        return relationships.get(relationshipType).topology().elementCount();
    }

    @Override
    public Set<RelationshipType> inverseIndexedRelationshipTypes() {
        return relationships
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().inverseTopology().isPresent())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    @Override
    public boolean hasRelationshipProperty(RelationshipType relType, String propertyKey) {
        return Optional
            .ofNullable(relationships.get(relType))
            .flatMap(SingleTypeRelationships::properties)
            .map(propertyStore -> propertyStore.containsKey(propertyKey))
            .orElse(false);
    }

    @Override
    public ValueType relationshipPropertyType(String propertyKey) {
        return relationships
            .values()
            .stream()
            .flatMap(relationship -> relationship.properties().stream())
            .filter(propertyStore -> propertyStore.containsKey(propertyKey))
            .map(propertyStore -> propertyStore.get(propertyKey).valueType())
            .findFirst()
            .orElse(ValueType.UNKNOWN);
    }

    @Override
    public Set<String> relationshipPropertyKeys() {
        return relationships
            .values()
            .stream()
            .flatMap(relationship -> relationship.properties().stream())
            .flatMap(propertyStore -> propertyStore.keySet().stream())
            .collect(Collectors.toSet());
    }

    @Override
    public Set<String> relationshipPropertyKeys(RelationshipType relationshipType) {
        return Optional
            .ofNullable(relationships.get(relationshipType))
            .flatMap(SingleTypeRelationships::properties)
            .map(RelationshipPropertyStore::keySet)
            .orElseGet(Set::of);
    }

    @Override
    public RelationshipProperty relationshipPropertyValues(RelationshipType relationshipType, String propertyKey) {
        return Optional
            .ofNullable(relationships.get(relationshipType))
            .flatMap(SingleTypeRelationships::properties)
            .map(propertyStore -> propertyStore.get(propertyKey))
            .orElseThrow(() -> new IllegalArgumentException("No relationship properties found for relationship type `" + relationshipType + "` and property key `" + propertyKey + "`."));
    }

    @Override
    public void addRelationshipType(
        RelationshipType relationshipType, SingleTypeRelationships relationships
    ) {
        updateGraphStore(graphStore -> {
            graphStore.relationships.computeIfAbsent(relationshipType, __ -> {
                var relationshipSchemaEntry = schema()
                    .relationshipSchema()
                    .getOrCreateRelationshipType(relationshipType, relationships.direction());
                relationships.updateRelationshipSchemaEntry(relationshipSchemaEntry);
                return relationships;
            });
        });
    }

    @Override
    public void addInverseIndex(
        RelationshipType relationshipType,
        SingleTypeRelationships indexedRelationships
    ) {
        var newRelationships =
            SingleTypeRelationships
                .builder()
                .from(relationships.get(relationshipType))
                .inverseTopology(indexedRelationships.topology())
                .inverseProperties(indexedRelationships.properties())
                .build();

        relationships.put(relationshipType, newRelationships);
    }

    @Override
    public DeletionResult deleteRelationships(RelationshipType relationshipType) {
        return DeletionResult.of(builder -> updateGraphStore(graphStore -> {
            Optional.ofNullable(graphStore.relationships.remove(relationshipType)).ifPresentOrElse(relationship -> {
                builder.deletedRelationships(relationship.topology().elementCount());
                relationship.properties().ifPresent(properties -> {
                    properties
                        .values()
                        .forEach(property -> builder.putDeletedProperty(property.key(),
                            property.values().elementCount()
                        ));
                });
                schema().relationshipSchema().remove(relationshipType);
            }, () -> builder.deletedRelationships(0));
        }));
    }

    @Override
    public CSRGraph getGraph(Collection<NodeLabel> nodeLabels) {
        return getGraph(nodeLabels, List.of(), Optional.empty());
    }

    @Override
    public CSRGraph getGraph(
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        Optional<String> maybeRelationshipProperty
    ) {
        validateInput(relationshipTypes, maybeRelationshipProperty);
        if (relationshipTypes.isEmpty()) {
            return createNodeOnlyGraph(nodeLabels);
        } else {
            return createGraph(nodeLabels, relationshipTypes, maybeRelationshipProperty);
        }
    }

    @Override
    public CSRGraph getUnion() {
        if (relationships.isEmpty()) {
            return getGraph(nodeLabels());
        }
        var graphs = relationships.entrySet().stream().flatMap(entry -> {
            var relationshipType = entry.getKey();
            var relationship = entry.getValue();

            return relationship
                .properties()
                .map(properties -> properties.keySet().stream().map(Optional::of))
                .orElseGet(() -> Stream.of(Optional.empty()))
                .map(propertyKey -> createGraph(nodeLabels(), relationshipType, propertyKey));
        }).collect(Collectors.toList());

        return UnionGraph.of(graphs);
    }

    @Override
    public void canRelease(boolean canRelease) {
        createdGraphs.forEach(graph -> graph.canRelease(canRelease));
    }

    @Override
    public CompositeRelationshipIterator getCompositeRelationshipIterator(
        RelationshipType relationshipType,
        Collection<String> propertyKeys
    ) {
        if (!relationshipTypes().contains(relationshipType)) {
            throw new IllegalArgumentException(prettySuggestions(
                formatWithLocale(
                    "Unknown relationship type `%s`.",
                    relationshipType
                ),
                relationshipType.name(),
                relationshipTypes().stream().map(RelationshipType::name).collect(Collectors.toSet())
            ));
        }

        var availableProperties = relationshipPropertyKeys(relationshipType);
        if (!availableProperties.containsAll(propertyKeys)) {
            var missingPropertyKeys = propertyKeys
                .stream()
                .filter(propertyKey -> !availableProperties.contains(propertyKey))
                .collect(Collectors.toList());
            throw new IllegalArgumentException(formatWithLocale(
                "Missing property keys %s for relationship type %s. Available property keys are %s",
                StringJoining.join(missingPropertyKeys),
                relationshipType.name,
                StringJoining.join(availableProperties)
            ));
        }

        var relationship = relationships.get(relationshipType);
        var adjacencyList = relationship.topology().adjacencyList();
        var inverseAdjacencyList = relationship
            .inverseTopology()
            .map(Topology::adjacencyList);
        var inverseProperties = relationship.inverseProperties()
            .map(propertyStore -> propertyKeys
                .stream()
                .map(propertyStore::get)
                .map(RelationshipProperty::values)
                .map(Properties::propertiesList)
                .toArray(AdjacencyProperties[]::new))
            .orElse(CSRCompositeRelationshipIterator.EMPTY_PROPERTIES);

        var properties = propertyKeys.isEmpty() ? CSRCompositeRelationshipIterator.EMPTY_PROPERTIES : propertyKeys
            .stream()
            .map(propertyKey -> relationshipPropertyValues(relationshipType, propertyKey))
            .map(RelationshipProperty::values)
            .map(Properties::propertiesList)
            .toArray(AdjacencyProperties[]::new);

        return new CSRCompositeRelationshipIterator(
            adjacencyList,
            inverseAdjacencyList,
            propertyKeys.toArray(new String[0]),
            properties,
            inverseProperties
        );
    }

    @Override
    public void release() {
        createdGraphs.forEach(Graph::release);
        releaseInternals();
    }

    private void releaseInternals() {
        var closeables = Stream.<AutoCloseable>builder();
        if (this.nodes instanceof AutoCloseable) {
            closeables.accept((AutoCloseable) this.nodes);
        }
        this.relationships.values().forEach(relationship -> {
            closeables.add(relationship.topology().adjacencyList());
            relationship
                .properties()
                .stream()
                .flatMap(properties -> properties.values().stream())
                .forEach(property -> closeables.add(property.values().propertiesList()));
        });

        ExceptionUtil.closeAll(ExceptionUtil.RETHROW_UNCHECKED, closeables.build().distinct());
    }

    @Override
    public long nodeCount() {
        return nodes.nodeCount();
    }

    private synchronized void updateGraphStore(Consumer<CSRGraphStore> updateFunction) {
        updateFunction.accept(this);
        this.modificationTime = TimeUtil.now();
    }

    private CSRGraph createGraph(
        Collection<NodeLabel> nodeLabels, RelationshipType relationshipType, Optional<String> maybeRelationshipProperty
    ) {
        var filteredNodes = getFilteredIdMap(nodeLabels);
        Map<String, NodePropertyValues> filteredNodeProperties = filterNodeProperties(nodeLabels);
        var nodeSchema = schema().nodeSchema().filter(new HashSet<>(nodeLabels));
        return createGraphFromRelationshipType(filteredNodes,
            filteredNodeProperties,
            nodeSchema,
            relationshipType,
            maybeRelationshipProperty
        );
    }

    private CSRGraph createGraph(
        Collection<NodeLabel> filteredLabels,
        Collection<RelationshipType> relationshipTypes,
        Optional<String> maybeRelationshipProperty
    ) {
        var filteredNodes = getFilteredIdMap(filteredLabels);
        Map<String, NodePropertyValues> filteredNodeProperties = filterNodeProperties(filteredLabels);
        var nodeSchema = schema().nodeSchema().filter(new HashSet<>(filteredLabels));

        List<CSRGraph> filteredGraphs = relationships
            .keySet()
            .stream()
            .filter(relationshipTypes::contains)
            .map(relationshipType -> createGraphFromRelationshipType(filteredNodes,
                filteredNodeProperties,
                nodeSchema,
                relationshipType,
                maybeRelationshipProperty
            ))
            .collect(Collectors.toList());

        filteredGraphs.forEach(graph -> graph.canRelease(false));
        createdGraphs.addAll(filteredGraphs);
        return UnionGraph.of(filteredGraphs);
    }

    private CSRGraph createNodeOnlyGraph(Collection<NodeLabel> nodeLabels) {
        var filteredNodes = getFilteredIdMap(nodeLabels);
        var filteredNodeProperties = filterNodeProperties(nodeLabels);
        var nodeSchema = schema().nodeSchema().filter(new HashSet<>(nodeLabels));

        var graphSchema = GraphSchema.of(nodeSchema, RelationshipSchema.empty(), schema.graphProperties());

        var initialGraph = new HugeGraphBuilder()
            .nodes(nodes)
            .schema(graphSchema)
            .characteristics(GraphCharacteristics.NONE)
            .nodeProperties(filteredNodeProperties)
            .topology(Topology.EMPTY)
            .build();

        return filteredNodes.isPresent() ? new NodeFilteredGraph(initialGraph, filteredNodes.get()) : initialGraph;
    }

    @NotNull
    private Optional<? extends FilteredIdMap> getFilteredIdMap(Collection<NodeLabel> filteredLabels) {
        boolean loadAllNodes = filteredLabels.containsAll(nodeLabels());

        return loadAllNodes || schema()
            .nodeSchema()
            .containsOnlyAllNodesLabel() ? Optional.empty() : nodes.withFilteredLabels(filteredLabels, concurrency);
    }

    private CSRGraph createGraphFromRelationshipType(
        Optional<? extends FilteredIdMap> filteredNodes,
        Map<String, NodePropertyValues> filteredNodeProperties,
        NodeSchema nodeSchema,
        RelationshipType relationshipType,
        Optional<String> maybeRelationshipProperty
    ) {
        var graphSchema = GraphSchema.of(nodeSchema,
            schema().relationshipSchema().filter(Set.of(relationshipType)),
            schema.graphProperties()
        );

        var relationship = relationships.get(relationshipType);
        var properties = maybeRelationshipProperty.map(propertyKey -> relationship
            .properties()
            .map(props -> props.get(propertyKey).values())
            .orElseThrow(() -> new IllegalArgumentException("Relationship property key not present in graph: " + propertyKey)));

        var inverseProperties = relationship
            .inverseProperties()
            .flatMap(inversePropertyStore -> maybeRelationshipProperty.map(propertyKey -> inversePropertyStore.get(propertyKey).values()));

        var characteristicsBuilder = GraphCharacteristics.builder().withDirection(schema.direction());
        relationship.inverseTopology().ifPresent(__ -> characteristicsBuilder.inverseIndexed());

        var initialGraph = new HugeGraphBuilder()
            .nodes(nodes)
            .schema(graphSchema)
            .characteristics(characteristicsBuilder.build())
            .nodeProperties(filteredNodeProperties)
            .topology(relationship.topology())
            .relationshipProperties(properties)
            .inverseTopology(relationship.inverseTopology())
            .inverseRelationshipProperties(inverseProperties)
            .build();

        return filteredNodes.isPresent() ? new NodeFilteredGraph(initialGraph, filteredNodes.get()) : initialGraph;
    }

    private Map<String, NodePropertyValues> filterNodeProperties(Collection<NodeLabel> labels) {
        if (this.nodeProperties.isEmpty()) {
            return Collections.emptyMap();
        }
        if (labels.size() == 1 || schema().nodeSchema().containsOnlyAllNodesLabel()) {
            return this.nodeProperties.propertyValues();
        }


        return schema()
            .nodeSchema()
            .filter(new HashSet<>(labels))
            .allProperties()
            .stream()
            .collect(toMap(Function.identity(), propertyKey -> nodeProperty(propertyKey).values()));
    }

    private void validateInput(
        Collection<RelationshipType> relationshipTypes, Optional<String> maybeRelationshipProperty
    ) {
        relationshipTypes.forEach(relationshipType -> {
            if (!relationships.containsKey(relationshipType)) {
                throw new IllegalArgumentException(formatWithLocale("No relationships have been loaded for relationship type '%s'",
                    relationshipType
                ));
            }

            maybeRelationshipProperty.ifPresent(relationshipProperty -> {
                if (!hasRelationshipProperty(relationshipType, relationshipProperty)) {
                    throw new IllegalArgumentException(formatWithLocale(
                        "Property '%s' does not exist for relationships with type '%s'.",
                        maybeRelationshipProperty.get(),
                        relationshipType
                    ));
                }
            });
        });
    }
}
