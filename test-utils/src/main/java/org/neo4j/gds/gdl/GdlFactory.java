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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.loading.Nodes;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.ImmutablePropertyConfig;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.PropertyValues;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlSupportPerMethodExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Element;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.utils.ContinuousId;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

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
        var config = graphProjectConfig.orElseGet(() -> ImmutableGraphProjectFromGdlConfig.builder()
            .username(userName.orElse(Username.EMPTY_USERNAME.username()))
            .graphName(graphName.orElse("graph"))
            .gdlGraph(gdlGraph.orElse(""))
            .build());

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

        return new GdlFactory(
            gdlHandler,
            config,
            graphDimensions,
            databaseId.orElse(GdlSupportPerMethodExtension.DATABASE_ID),
            capabilities
        );
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
    protected MutableGraphSchema computeGraphSchema(Nodes nodes, RelationshipImportResult relationshipImportResult) {
        var relationshipSchema = relationshipImportResult.importResults().entrySet().stream().reduce(
            MutableRelationshipSchema.empty(),
            (unionSchema, entry) -> {
                var relationshipType = entry.getKey();
                var relationships = entry.getValue();
                return unionSchema.union(relationships.relationshipSchema(relationshipType));
            },
            MutableRelationshipSchema::union
        );

        return MutableGraphSchema.of(
            nodes.schema(),
            relationshipSchema,
            Map.of()
        );
    }

    @Override
    public CSRGraphStore build() {
        var nodes = loadNodes();
        var relationshipImportResult = loadRelationships(nodes.idMap());
        var schema = computeGraphSchema(nodes, relationshipImportResult);

        return new GraphStoreBuilder()
            .databaseId(databaseId)
            .capabilities(capabilities)
            .schema(schema)
            .nodes(nodes)
            .relationshipImportResult(relationshipImportResult)
            .concurrency(1)
            .build();
    }

    private Nodes loadNodes() {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(dimensions.highestPossibleNodeCount() - 1)
            .hasLabelInformation(true)
            .hasProperties(true)
            .concurrency(1)
            .propertyState(graphProjectConfig.propertyState())
            .build();

        gdlHandler.getVertices().forEach(vertex -> {
            var labels = vertex.getLabels();
            if (labels.contains(NodeLabel.ALL_NODES.name())) {
                labels = labels
                    .stream()
                    .filter(label -> !NodeLabel.ALL_NODES.name().equals(label))
                    .collect(Collectors.toList());
            }

            Map<String, Value> propertyValues = new HashMap<>();
            vertex.getProperties().forEach((propertyKey, propertyValue) -> {
                if (propertyValue instanceof List) {
                    propertyValue = convertListProperty((List<?>) propertyValue);
                }
                propertyValues.put(propertyKey, Values.of(propertyValue));
            });

            nodesBuilder.addNode(
                vertex.getId(),
                NodeLabelTokens.of(labels),
                PropertyValues.of(propertyValues)
            );
        });

        return nodesBuilder.build();
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
    
    private RelationshipImportResult loadRelationships(IdMap idMap) {
        var propertyKeysByRelType = propertyKeysByRelType();
        var relationshipBuilders = createRelationshipBuilders(idMap, propertyKeysByRelType);

        importRelationships(propertyKeysByRelType, relationshipBuilders);

        var resultBuilder = RelationshipImportResult.builder();
        relationshipBuilders.forEach((relationshipType, relationshipsBuilder) -> {
            resultBuilder.putImportResult(relationshipType, relationshipsBuilder.build());
        });

        return resultBuilder.build();
    }

    @NotNull
    private HashMap<RelationshipType, List<String>> propertyKeysByRelType() {
        var propertyKeysByRelType = new HashMap<RelationshipType, List<String>>();

        Direction orientation = Direction.fromOrientation(graphProjectConfig.orientation());
        var relationshipSchema = MutableRelationshipSchema.empty();
        gdlHandler.getEdges().forEach(edge -> {
            var relType = RelationshipType.of(edge.getLabel());
            var entry = relationshipSchema.getOrCreateRelationshipType(relType, orientation);
            edge.getProperties().keySet().forEach(propertyKey ->
                entry.addProperty(propertyKey, ValueType.DOUBLE, PropertyState.PERSISTENT)
            );
        });

        relationshipSchema
            .availableTypes()
            .forEach(relType -> propertyKeysByRelType.put(
                relType,
                new ArrayList<>(relationshipSchema.allProperties(relType))
            ));

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
                        .map(propertyKey -> ImmutablePropertyConfig.builder()
                            .propertyKey(propertyKey)
                            .aggregation(graphProjectConfig.aggregation())
                            .propertyState(graphProjectConfig.propertyState())
                            .defaultValue(DefaultValue.forDouble())
                            .build()
                        )
                        .collect(Collectors.toList());

                    return GraphFactory.initRelationshipsBuilder()
                        .nodes(idMap)
                        .orientation(graphProjectConfig.orientation())
                        .aggregation(graphProjectConfig.aggregation())
                        .indexInverse(graphProjectConfig.indexInverse())
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
