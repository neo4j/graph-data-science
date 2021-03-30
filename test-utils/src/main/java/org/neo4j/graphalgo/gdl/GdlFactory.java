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
package org.neo4j.graphalgo.gdl;

import org.immutables.builder.Builder;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.CSRGraphStoreFactory;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphLoaderContext;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipProperty;
import org.neo4j.graphalgo.api.RelationshipPropertyStore;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.loading.CSRGraphStore;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.extension.GdlSupportExtension;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.values.storable.NumberType;
import org.neo4j.values.storable.Values;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Element;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.utils.ContinuousId;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class GdlFactory extends CSRGraphStoreFactory<GraphCreateFromGdlConfig> {

    private final GDLHandler gdlHandler;
    private final NamedDatabaseId databaseId;

    public static GdlFactory of(String gdlGraph) {
        return builder().gdlGraph(gdlGraph).build();
    }

    public static GdlFactoryBuilder builder() {
        return new GdlFactoryBuilder();
    }

    @Builder.Factory
    static GdlFactory gdlFactory(
        Optional<String> gdlGraph,
        Optional<NamedDatabaseId> namedDatabaseId,
        Optional<String> userName,
        Optional<String> graphName,
        Optional<GraphCreateFromGdlConfig> createConfig,
        Optional<LongSupplier> nodeIdFunction
    ) {
        var config = createConfig.isEmpty()
            ? ImmutableGraphCreateFromGdlConfig.builder()
            .username(userName.orElse(AuthSubject.ANONYMOUS.username()))
            .graphName(graphName.orElse("graph"))
            .gdlGraph(gdlGraph.orElse(""))
            .build()
            : createConfig.get();

        var databaseId = namedDatabaseId.orElse(GdlSupportExtension.DATABASE_ID);

        var nextVertexId = nodeIdFunction
            .map(supplier -> (Function<Optional<String>, Long>) (ignored) -> supplier.getAsLong())
            .orElseGet(ContinuousId::new);

        var gdlHandler = new GDLHandler.Builder()
            .setNextVertexId(nextVertexId)
            .setDefaultVertexLabel(NodeLabel.ALL_NODES.name)
            .setDefaultEdgeLabel(RelationshipType.ALL_RELATIONSHIPS.name)
            .buildFromString(config.gdlGraph());

        var graphDimensions = GraphDimensionsGdlReader.of(gdlHandler);

        return new GdlFactory(gdlHandler, config, graphDimensions, databaseId);
    }

    private GdlFactory(
        GDLHandler gdlHandler,
        GraphCreateFromGdlConfig graphCreateConfig,
        GraphDimensions graphDimensions,
        NamedDatabaseId databaseId
    ) {
        super(graphCreateConfig, NO_API_CONTEXT, graphDimensions);
        this.gdlHandler = gdlHandler;
        this.databaseId = databaseId;
    }

    public long nodeId(String variable) {
        return gdlHandler.getVertexCache().get(variable).getId();
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.empty();
    }

    @Override
    protected ProgressLogger initProgressLogger() {
        return ProgressLogger.NULL_LOGGER;
    }

    @Override
    public ImportResult<CSRGraphStore> build() {
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
                        GraphStore.PropertyState.PERSISTENT,
                        propertyValues,
                        DefaultValue.forDouble(),
                        graphCreateConfig.aggregation()
                    )
                );
            });

            topologies.put(loadResult.relationshipType(), loadResult.topology());
            properties.put(loadResult.relationshipType(), builder.build());

        });

        CSRGraphStore graphStore = CSRGraphStore.of(
            databaseId,
            nodes.idMap(),
            nodes.properties(),
            topologies,
            properties,
            1,
            loadingContext.tracker()
        );
        return ImportResult.of(dimensions, graphStore);
    }

    private IdsAndProperties loadNodes() {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(dimensions.highestNeoId())
            .hasLabelInformation(true)
            .concurrency(1)
            .tracker(loadingContext.tracker())
            .build();

        gdlHandler.getVertices().forEach(vertex -> nodesBuilder.addNode(
            vertex.getId(),
            vertex.getLabels().stream()
                .map(NodeLabel::of)
                .filter(nodeLabel -> !nodeLabel.equals(NodeLabel.ALL_NODES))
                .toArray(NodeLabel[]::new)
        ));

        var idMap = nodesBuilder.build().nodeMapping();

        return IdsAndProperties.of(idMap, loadNodeProperties(idMap));
    }

    private Map<NodeLabel, Map<PropertyMapping, NodeProperties>> loadNodeProperties(IdMapping idMap) {
        var propertyKeysByLabel = new HashMap<NodeLabel, Set<PropertyMapping>>();
        var propertyBuilders = new HashMap<PropertyMapping, NodePropertiesFromStoreBuilder>();

        gdlHandler.getVertices().forEach(vertex -> vertex
            .getProperties()
            .forEach((propertyKey, propertyValue) -> {
                vertex.getLabels().stream()
                    .map(NodeLabel::of)
                    .forEach(nodeLabel -> propertyKeysByLabel
                        .computeIfAbsent(nodeLabel, (ignore) -> new HashSet<>())
                        .add(PropertyMapping.of(propertyKey))
                    );

                if (propertyValue instanceof List) {
                    propertyValue = convertListProperty((List<?>) propertyValue);
                }

                propertyBuilders.computeIfAbsent(PropertyMapping.of(propertyKey), (key) ->
                    NodePropertiesFromStoreBuilder.of(
                        dimensions.nodeCount(),
                        loadingContext.tracker(),
                        DefaultValue.DEFAULT
                    )).set(idMap.toMappedNodeId(vertex.getId()), Values.of(propertyValue));
            }));

        Map<PropertyMapping, NodeProperties> nodeProperties = propertyBuilders
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().build()));

        return propertyKeysByLabel.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream().collect(Collectors.toMap(
                    propertyKey -> propertyKey,
                    nodeProperties::get
                ))
            ));
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

    private List<RelationshipsLoadResult> loadRelationships(IdMapping nodeMapping) {
        var propertyKeysByRelType = propertyKeysByRelType();
        var relationshipBuilders = createRelationshipBuilders(nodeMapping, propertyKeysByRelType);

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

        var schemaBuilder = RelationshipSchema.builder();
        gdlHandler.getEdges().forEach(edge -> {
            var relType = RelationshipType.of(edge.getLabel());
            schemaBuilder.addRelationshipType(relType);
            edge.getProperties().keySet().forEach(propertyKey ->
                schemaBuilder.addProperty(relType, propertyKey, ValueType.DOUBLE)
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
        IdMapping nodeMapping,
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
                            graphCreateConfig.aggregation(),
                            DefaultValue.forDouble()
                        ))
                        .collect(Collectors.toList());

                    return GraphFactory.initRelationshipsBuilder()
                        .nodes(nodeMapping)
                        .orientation(graphCreateConfig.orientation())
                        .aggregation(graphCreateConfig.aggregation())
                        .addAllPropertyConfigs(propertyConfigs)
                        .executorService(loadingContext.executor())
                        .tracker(loadingContext.tracker())
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

    private static final GraphLoaderContext NO_API_CONTEXT = new GraphLoaderContext() {
        @Override
        public GraphDatabaseAPI api() {
            return null;
        }

        @Override
        public Log log() {
            return NullLog.getInstance();
        }
    };

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
                .highestNeoId(highestId)
                .maxRelCount(relCount)
                .build();
        }
    }
}
