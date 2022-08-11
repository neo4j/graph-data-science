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
package org.neo4j.gds.gdl;

import org.immutables.builder.Builder;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.IdMapAndProperties;
import org.neo4j.gds.core.loading.ImmutableRelationshipsAndProperties;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.loading.RelationshipsAndProperties;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlSupportPerMethodExtension;
import org.neo4j.values.storable.NumberType;
import org.neo4j.values.storable.Values;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Element;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.utils.ContinuousId;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class GdlFactory extends CSRGraphStoreFactory<GraphProjectFromGdlConfig> {

    private final GDLHandler gdlHandler;
    private final DatabaseId databaseId;

    public static GdlFactory of(String gdlGraph) {
        return builder().gdlGraph(gdlGraph).build();
    }

    public static GdlFactoryBuilder builder() {
        return new GdlFactoryBuilder();
    }

    @Builder.Factory
    static GdlFactory gdlFactory(
        Optional<String> gdlGraph,
        Optional<DatabaseId> databaseId,
        Optional<String> userName,
        Optional<String> graphName,
        Optional<GraphProjectFromGdlConfig> graphProjectConfig,
        Optional<LongSupplier> nodeIdFunction,
        Optional<Capabilities> graphCapabilities
    ) {
        var config = graphProjectConfig.isEmpty()
            ? ImmutableGraphProjectFromGdlConfig.builder()
            .username(userName.orElse(Username.EMPTY_USERNAME.username()))
            .graphName(graphName.orElse("graph"))
            .gdlGraph(gdlGraph.orElse(""))
            .build()
            : graphProjectConfig.get();

        var nextVertexId = nodeIdFunction
            .map(supplier -> (Function<Optional<String>, Long>) (ignored) -> supplier.getAsLong())
            .orElseGet(ContinuousId::new);

        var gdlHandler = new GDLHandler.Builder()
            .setNextVertexId(nextVertexId)
            .setDefaultVertexLabel(NodeLabel.ALL_NODES.name)
            .setDefaultEdgeLabel(RelationshipType.ALL_RELATIONSHIPS.name)
            .buildFromString(config.gdlGraph());

        var graphDimensions = GraphDimensionsGdlReader.of(gdlHandler);

        // NOTE: We don't really have a database, but GDL is for testing to work as if we had a database
        var capabilities = graphCapabilities.orElseGet(() -> ImmutableStaticCapabilities.of(true));

        return new GdlFactory(gdlHandler, config, graphDimensions, databaseId.orElse(GdlSupportPerMethodExtension.DATABASE_ID), capabilities);
    }

    private GdlFactory(
        GDLHandler gdlHandler,
        GraphProjectFromGdlConfig graphProjectConfig,
        GraphDimensions graphDimensions,
        DatabaseId databaseId,
        Capabilities capabilities
    ) {
        super(
            graphProjectConfig,
            capabilities,
            GraphLoaderContext.NULL_CONTEXT,
            graphDimensions
        );
        this.gdlHandler = gdlHandler;
        this.databaseId = databaseId;
    }

    public long nodeId(String variable) {
        return gdlHandler.getVertexCache().get(variable).getId();
    }

    @Override
    public MemoryEstimation estimateMemoryUsageDuringLoading() {
        return MemoryEstimations.empty();
    }

    @Override
    public MemoryEstimation estimateMemoryUsageAfterLoading() {
        return MemoryEstimations.empty();
    }

    @Override
    protected ProgressTracker initProgressTracker() {
        return ProgressTracker.NULL_TRACKER;
    }

    @Override
    protected GraphSchema computeGraphSchema(
        IdMapAndProperties idMapAndProperties, RelationshipsAndProperties relationshipsAndProperties
    ) {
        var nodeProperties = idMapAndProperties.properties();
        NodeSchema.Builder nodeSchemaBuilder = NodeSchema.builder();
        gdlHandler
            .getVertices()
            .forEach(vertex -> {
                var labels = vertex.getLabels().stream().map(NodeLabel::of).collect(Collectors.toList());
                if (labels.isEmpty()) {
                    labels = List.of(NodeLabel.ALL_NODES);
                }

                labels.forEach(label -> vertex
                    .getProperties()
                    .forEach((propertyKey, propertyValue) -> nodeSchemaBuilder.addProperty(
                        label,
                        propertyKey,
                        nodeProperties.get(propertyKey).valueType()
                    )));
            });
        // in case there were no properties add all labels
        idMapAndProperties.idMap().availableNodeLabels().forEach(nodeSchemaBuilder::addLabel);

        boolean isUndirected = graphProjectConfig.orientation() == Orientation.UNDIRECTED;
        var relationshipSchemaBuilder = RelationshipSchema.builder();
        relationshipsAndProperties
            .properties()
            .forEach((relType, propertyStore) -> propertyStore
                .relationshipProperties()
                .forEach((propertyKey, propertyValues) -> relationshipSchemaBuilder.addProperty(
                    relType,
                    isUndirected,
                    propertyKey,
                    RelationshipPropertySchema.of(
                        propertyKey,
                        propertyValues.valueType(),
                        propertyValues.valueType().fallbackValue(),
                        PropertyState.PERSISTENT,
                        graphProjectConfig.aggregation()
                    )
                )));
        relationshipsAndProperties
            .relationships()
            .keySet()
            .forEach(type -> relationshipSchemaBuilder.addRelationshipType(type, isUndirected));

        return GraphSchema.of(
            nodeSchemaBuilder.build(),
            relationshipSchemaBuilder.build(),
            Map.of()
        );
    }

    @Override
    public CSRGraphStore build() {
        var nodes = loadNodes();
        var relationships = loadRelationships(nodes.idMap());

        var topologies = new HashMap<RelationshipType, Relationships.Topology>();
        var properties = new HashMap<RelationshipType, RelationshipPropertyStore>();

        relationships.forEach(loadResult -> {
            var builder = RelationshipPropertyStore.builder();
            loadResult.properties().forEach((propertyKey, propertyValues) -> {
                builder.putIfAbsent(
                    propertyKey,
                    RelationshipProperty.of(
                        propertyKey,
                        NumberType.FLOATING_POINT,
                        PropertyState.PERSISTENT,
                        propertyValues,
                        DefaultValue.forDouble(),
                        graphProjectConfig.aggregation()
                    )
                );
            });

            topologies.put(loadResult.relationshipType(), loadResult.topology());
            properties.put(loadResult.relationshipType(), builder.build());
        });

        var schema = computeGraphSchema(nodes, ImmutableRelationshipsAndProperties.of(topologies, properties));

        return new GraphStoreBuilder()
            .databaseId(databaseId)
            .capabilities(capabilities)
            .schema(schema)
            .nodes(nodes.idMap())
            .nodePropertyStore(nodes.properties())
            .relationships(topologies)
            .relationshipPropertyStores(properties)
            .concurrency(1)
            .build();
    }

    private IdMapAndProperties loadNodes() {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(dimensions.highestPossibleNodeCount() - 1)
            .hasLabelInformation(true)
            .concurrency(1)
            .build();

        gdlHandler.getVertices().forEach(vertex -> {
            var labels = vertex.getLabels();
            if (labels.contains(NodeLabel.ALL_NODES.name())) {
                labels = labels
                    .stream()
                    .filter(label -> !NodeLabel.ALL_NODES.name().equals(label))
                    .collect(Collectors.toList());
            }
            nodesBuilder.addNode(
                vertex.getId(),
                NodeLabelTokens.of(labels)
            );
        });

        var idMap = nodesBuilder.build().idMap();

        return IdMapAndProperties.of(idMap, loadNodeProperties(idMap));
    }

    private Map<PropertyMapping, NodePropertyValues> loadNodeProperties(IdMap idMap) {
        var propertyBuilders = new HashMap<PropertyMapping, NodePropertiesFromStoreBuilder>();

        gdlHandler.getVertices().forEach(vertex -> vertex
            .getProperties()
            .forEach((propertyKey, propertyValue) -> {
                if (propertyValue instanceof List) {
                    propertyValue = convertListProperty((List<?>) propertyValue);
                }

                propertyBuilders.computeIfAbsent(PropertyMapping.of(propertyKey), (key) ->
                    NodePropertiesFromStoreBuilder.of(
                        DefaultValue.DEFAULT,
                        1
                    )).set(vertex.getId(), Values.of(propertyValue));
            }));

        return propertyBuilders
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().build(idMap)));
    }

    @NotNull
    private Object convertListProperty(List<?> list) {
        var firstType = list.get(0).getClass();

        var isLong = firstType.equals(Long.class);
        var isDouble = firstType.equals(Double.class);
        var isFloat = firstType.equals(Float.class);

        if (!isLong && !isDouble && !isFloat) {
            throw new IllegalArgumentException(formatWithLocale(
                "List property contains in-compatible type: %s.",
                firstType.getSimpleName()
            ));
        }

        var sameType = list.stream().allMatch(firstType::isInstance);
        if (!sameType) {
            throw new IllegalArgumentException(formatWithLocale(
                "List property contains mixed types: %s",
                list
                    .stream()
                    .map(Object::getClass)
                    .map(Class::getSimpleName)
                    .collect(Collectors.joining(", ", "[", "]"))
            ));
        }

        var array = Array.newInstance(firstType, list.size());
        for (int i = 0; i < list.size(); i++) {
            Array.set(array, i, firstType.cast(list.get(i)));
        }
        return array;
    }

    @ValueClass
    interface RelationshipsLoadResult {
        RelationshipType relationshipType();

        Relationships.Topology topology();

        Map<String, Relationships.Properties> properties();
    }

    private List<RelationshipsLoadResult> loadRelationships(IdMap idMap) {
        var propertyKeysByRelType = propertyKeysByRelType();
        var relationshipBuilders = createRelationshipBuilders(idMap, propertyKeysByRelType);

        importRelationships(propertyKeysByRelType, relationshipBuilders);

        return relationshipBuilders.entrySet()
            .stream()
            .map(entry -> {
                    var relationships = entry.getValue().buildAll();

                    var topology = relationships.get(0).topology();
                    var propertyKeys = propertyKeysByRelType.get(entry.getKey());

                    var properties = IntStream.range(0, propertyKeys.size())
                        .boxed()
                        .collect(Collectors.toMap(propertyKeys::get, idx -> relationships.get(idx).properties().get()));

                    return ImmutableRelationshipsLoadResult.builder()
                        .relationshipType(entry.getKey())
                        .topology(topology)
                        .properties(properties)
                        .build();
                }
            ).collect(Collectors.toList());
    }

    @NotNull
    private HashMap<RelationshipType, List<String>> propertyKeysByRelType() {
        var propertyKeysByRelType = new HashMap<RelationshipType, List<String>>();

        boolean isUndirected = graphProjectConfig.orientation() == Orientation.UNDIRECTED;
        var schemaBuilder = RelationshipSchema.builder();
        gdlHandler.getEdges().forEach(edge -> {
            var relType = RelationshipType.of(edge.getLabel());
            schemaBuilder.addRelationshipType(relType, isUndirected);
            edge.getProperties().keySet().forEach(propertyKey ->
                schemaBuilder.addProperty(relType, isUndirected, propertyKey, ValueType.DOUBLE)
            );
        });
        var schema = schemaBuilder.build();

        schema.properties().forEach((relType, properties) -> {
            propertyKeysByRelType.put(relType, properties.keySet().stream().sorted().collect(Collectors.toList()));
        });
        return propertyKeysByRelType;
    }

    private void importRelationships(
        Map<RelationshipType, List<String>> propertyKeysByRelType,
        Map<RelationshipType, RelationshipsBuilder> relationshipBuilders
    ) {
        gdlHandler.getEdges()
            .forEach(edge -> {
                var relType = RelationshipType.of(edge.getLabel());
                var relationshipsBuilder = relationshipBuilders.get(relType);
                var propertyKeys = propertyKeysByRelType.get(relType);

                if (propertyKeys.isEmpty()) {
                    relationshipsBuilder.add(edge.getSourceVertexId(), edge.getTargetVertexId());
                } else if (propertyKeys.size() == 1) {
                    relationshipsBuilder.add(
                        edge.getSourceVertexId(),
                        edge.getTargetVertexId(),
                        gdsValue(edge, propertyKeys.get(0), edge.getProperties().get(propertyKeys.get(0)))
                    );
                } else {
                    var values = propertyKeys
                        .stream()
                        .map(key -> gdsValue(edge, key, edge.getProperties().get(key)))
                        .mapToDouble(d -> d)
                        .toArray();
                    relationshipsBuilder.add(edge.getSourceVertexId(), edge.getTargetVertexId(), values);
                }
            });
    }

    @NotNull
    private Map<RelationshipType, RelationshipsBuilder> createRelationshipBuilders(
        IdMap idMap,
        Map<RelationshipType, List<String>> propertyKeysByRelType
    ) {
        return propertyKeysByRelType.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                relTypeAndProperty -> {
                    var propertyKeys = relTypeAndProperty.getValue();
                    var propertyConfigs = propertyKeys
                        .stream()
                        .map(key -> GraphFactory.PropertyConfig.of(
                            graphProjectConfig.aggregation(),
                            DefaultValue.forDouble()
                        ))
                        .collect(Collectors.toList());

                    return GraphFactory.initRelationshipsBuilder()
                        .nodes(idMap)
                        .orientation(graphProjectConfig.orientation())
                        .aggregation(graphProjectConfig.aggregation())
                        .addAllPropertyConfigs(propertyConfigs)
                        .executorService(loadingContext.executor())
                        .build();
                }
            ));
    }

    private double gdsValue(Element element, String propertyKey, Object gdlValue) {
        if (gdlValue == null) {
            return DefaultValue.forDouble().doubleValue();
        } else if (gdlValue instanceof Number) {
            return ((Number) gdlValue).doubleValue();
        } else if (gdlValue instanceof String && gdlValue.equals("NaN")) {
            return Double.NaN;
        } else {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "%s property '%s' must be of type Number, but was %s for %s.",
                element.getClass().getTypeName(),
                propertyKey,
                gdlValue.getClass(),
                element
            ));
        }
    }

    private static final class GraphDimensionsGdlReader {

        static GraphDimensions of(GDLHandler gdlHandler) {
            var nodeCount = gdlHandler.getVertices().size();
            var highestId = gdlHandler
                .getVertices()
                .stream()
                .map(Vertex::getId)
                .max(Long::compareTo)
                .orElse((long) nodeCount);
            var relCount = gdlHandler.getEdges().size();

            return ImmutableGraphDimensions.builder()
                .nodeCount(nodeCount)
                .highestPossibleNodeCount(highestId + 1)
                .relCountUpperBound(relCount)
                .build();
        }
    }
}
