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
package org.neo4j.gds.projection;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ReturnType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.ImmutableGraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.compress.AdjacencyCompressor;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.loading.LazyIdMapBuilder;
import org.neo4j.gds.core.loading.ReadHelper;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImportResult;
import org.neo4j.gds.core.loading.ValueConverter;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.PropertyValues;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class CypherAggregation {

    @Context
    public GraphDatabaseService databaseService;

    @Context
    public Username username = Username.EMPTY_USERNAME;

    @UserAggregationFunction(name = "gds.alpha.graph.project")
    @Description("Creates a named graph in the catalog for use by algorithms.")
    public GraphAggregator projectFromCypherAggregation() {
        var progressTimer = ProgressTimer.start();
        return new GraphAggregator(
            progressTimer,
            DatabaseId.of(this.databaseService),
            username.username()
        );
    }

    // public is required for the Cypher runtime
    @SuppressWarnings("WeakerAccess")
    public static class GraphAggregator {

        private final ProgressTimer progressTimer;
        private final DatabaseId databaseId;
        private final String username;

        // #result() may be called twice, we cache the result of the first call to return it again in the second invocation
        private @Nullable AggregationResult result;

        // Used for initializing the data and rel importers
        private final Lock lock;
        private volatile @Nullable CypherAggregation.LazyImporter importer;

        GraphAggregator(
            ProgressTimer progressTimer,
            DatabaseId databaseId,
            String username
        ) {
            this.progressTimer = progressTimer;
            this.databaseId = databaseId;
            this.username = username;
            this.lock = new ReentrantLock();
        }

        @UserAggregationUpdate
        public void update(
            @Name("graphName") String graphName,
            @Name("sourceNode") Object sourceNode,
            @Nullable @Name(value = "targetNode", defaultValue = "null") Object targetNode,
            @Nullable @Name(value = "nodesConfig", defaultValue = "null") Map<String, Object> nodesConfig,
            @Nullable @Name(value = "relationshipConfig", defaultValue = "null") Map<String, Object> relationshipConfig,
            @Nullable @Name(value = "configuration", defaultValue = "null") Map<String, Object> config
        ) {
            @Nullable Map<String, Value> sourceNodePropertyValues = null;
            @Nullable Map<String, Value> targetNodePropertyValues = null;
            NodeLabelToken sourceNodeLabels = NodeLabelTokens.missing();
            NodeLabelToken targetNodeLabels = NodeLabelTokens.missing();

            if (nodesConfig != null) {
                sourceNodePropertyValues = LazyImporter.propertiesConfig("sourceNodeProperties", nodesConfig);
                sourceNodeLabels = labelsConfig(sourceNode, "sourceNodeLabels", nodesConfig);

                if (targetNode != null) {
                    targetNodePropertyValues = LazyImporter.propertiesConfig("targetNodeProperties", nodesConfig);
                    targetNodeLabels = labelsConfig(targetNode, "targetNodeLabels", nodesConfig);
                }

                if (!nodesConfig.isEmpty()) {
                    CypherMapWrapper.create(nodesConfig).requireOnlyKeysFrom(List.of(
                        "sourceNodeProperties",
                        "sourceNodeLabels",
                        "targetNodeProperties",
                        "targetNodeLabels"
                    ));
                }
            }

            var data = initGraphData(
                graphName,
                config,
                targetNodePropertyValues,
                sourceNodePropertyValues,
                targetNodeLabels,
                sourceNodeLabels,
                relationshipConfig
            );

            data.update(
                sourceNode,
                targetNode,
                sourceNodePropertyValues,
                targetNodePropertyValues,
                sourceNodeLabels,
                targetNodeLabels,
                relationshipConfig
            );
        }

        private LazyImporter initGraphData(
            String graphName,
            @Nullable Map<String, Object> config,
            @Nullable Map<String, Value> sourceNodePropertyValues,
            @Nullable Map<String, Value> targetNodePropertyValues,
            NodeLabelToken sourceNodeLabels,
            NodeLabelToken targetNodeLabels,
            @Nullable Map<String, Object> relationshipConfig
        ) {
            var data = this.importer;
            if (data != null) {
                return data;
            }

            this.lock.lock();
            try {
                data = this.importer;
                if (data == null) {
                    this.importer = data = LazyImporter.of(
                        graphName,
                        this.username,
                        this.databaseId,
                        config,
                        sourceNodePropertyValues,
                        targetNodePropertyValues,
                        sourceNodeLabels,
                        targetNodeLabels,
                        relationshipConfig,
                        this.lock
                    );
                }
                return data;
            } finally {
                this.lock.unlock();
            }
        }


        private static NodeLabelToken labelsConfig(
            Object node,
            String nodeLabelKey,
            @NotNull Map<String, Object> nodesConfig
        ) {
            var nodeLabelsEntry = nodesConfig.remove(nodeLabelKey);
            return tryLabelsConfig(node, nodeLabelsEntry, nodeLabelKey);
        }

        private static NodeLabelToken tryLabelsConfig(
            Object node,
            @Nullable Object nodeLabels,
            String nodeLabelKey
        ) {
            if (Boolean.FALSE.equals(nodeLabels)) {
                return NodeLabelTokens.empty();
            }

            if (Boolean.TRUE.equals(nodeLabels)) {
                if (!(node instanceof Node)) {
                    throw new IllegalArgumentException(
                        "Using `true` to load all labels does only work if the node is a Neo4j node object"
                    );
                }
                nodeLabels = ((Node) node).getLabels();
            }

            var nodeLabelToken = NodeLabelTokens.ofNullable(nodeLabels);

            if (nodeLabelToken.isInvalid()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The value of `%s` must be either a `List of Strings`, a `String`, or a `Boolean`, but was `%s`.",
                    nodeLabelKey,
                    nodeLabels != null ? nodeLabels.getClass().getSimpleName() : "null"
                ));
            }

            return nodeLabelToken;
        }

        @UserAggregationResult
        @ReturnType(AggregationResult.class)
        public @Nullable Map<String, Object> result() {
            AggregationResult result = buildGraph();
            return result == null ? null : result.toMap();
        }

        public @Nullable AggregationResult buildGraph() {
            var importer = this.importer;
            if (importer == null) {
                // Nothing aggregated
                return null;
            }

            // Older cypher runtimes call the result method multiple times, we cache the result of the first call
            if (this.result != null) {
                return this.result;
            }

            this.result = importer.result(this.username, this.databaseId, this.progressTimer);

            return this.result;
        }
    }

    // Does the actual importing work, but is lazily initialized with the first row
    @SuppressWarnings("CodeBlock2Expr")
    private static final class LazyImporter {
        private final String graphName;
        private final GraphProjectFromCypherAggregationConfig config;
        private final LazyIdMapBuilder idMapBuilder;
        private final @Nullable List<RelationshipPropertySchema> relationshipPropertySchemas;

        private final Lock lock;
        private final Map<RelationshipType, RelationshipsBuilder> relImporters;
        private final ImmutableGraphSchema.Builder graphSchemaBuilder;

        private LazyImporter(
            String graphName,
            GraphProjectFromCypherAggregationConfig config,
            LazyIdMapBuilder idMapBuilder,
            @Nullable List<RelationshipPropertySchema> relationshipPropertySchemas,
            Lock lock
        ) {
            this.graphName = graphName;
            this.config = config;
            this.idMapBuilder = idMapBuilder;
            this.relationshipPropertySchemas = relationshipPropertySchemas;
            this.lock = lock;
            this.relImporters = new ConcurrentHashMap<>();
            this.graphSchemaBuilder = ImmutableGraphSchema.builder();
        }

        static LazyImporter of(
            String graphName,
            String username,
            DatabaseId databaseId,
            @Nullable Map<String, Object> configMap,
            @Nullable Map<String, Value> sourceNodePropertyValues,
            @Nullable Map<String, Value> targetNodePropertyValues,
            NodeLabelToken sourceNodeLabels,
            NodeLabelToken targetNodeLabels,
            @Nullable Map<String, Object> relationshipConfig,
            Lock lock
        ) {

            validateGraphName(graphName, username, databaseId);
            var config = GraphProjectFromCypherAggregationConfig.of(
                username,
                graphName,
                configMap
            );

            var idMapBuilder = idMapBuilder(
                sourceNodeLabels,
                sourceNodePropertyValues,
                targetNodeLabels,
                targetNodePropertyValues,
                config.readConcurrency()
            );

            var relationshipPropertySchemas = relationshipPropertySchemas(relationshipConfig);

            return new LazyImporter(graphName, config, idMapBuilder, relationshipPropertySchemas, lock);
        }

        private static void validateGraphName(String graphName, String username, DatabaseId databaseId) {
            if (GraphStoreCatalog.exists(username, databaseId, graphName)) {
                throw new IllegalArgumentException("Graph " + graphName + " already exists");
            }
        }

        private static LazyIdMapBuilder idMapBuilder(
            NodeLabelToken sourceNodeLabels,
            @Nullable Map<String, Value> sourceNodeProperties,
            NodeLabelToken targetNodeLabels,
            @Nullable Map<String, Value> targetNodeProperties,
            int readConcurrency
        ) {
            boolean hasLabelInformation = !(sourceNodeLabels.isMissing() && targetNodeLabels.isMissing());
            boolean hasProperties = !(sourceNodeProperties == null && targetNodeProperties == null);
            return new LazyIdMapBuilder(readConcurrency, hasLabelInformation, hasProperties);
        }

        private static @Nullable List<RelationshipPropertySchema> relationshipPropertySchemas(@Nullable Map<String, Object> relationshipConfig) {
            if (relationshipConfig == null) {
                return null;
            }

            var relationshipPropertySchemas = new ArrayList<RelationshipPropertySchema>();

            // We need to do this before extracting the `relationshipProperties`, because
            // we remove the original entry from the map during converting; also we remove null keys
            // so we could not create a schema entry for properties that are absent on the current relationship
            var relationshipPropertyKeys = relationshipConfig.get("properties");
            if (relationshipPropertyKeys instanceof Map) {
                for (var propertyKey : ((Map<?, ?>) relationshipPropertyKeys).keySet()) {
                    relationshipPropertySchemas.add(RelationshipPropertySchema.of(
                        String.valueOf(propertyKey),
                        ValueType.DOUBLE
                    ));
                }
            }

            if (relationshipPropertySchemas.isEmpty()) {
                return null;
            }

            return relationshipPropertySchemas;
        }

        void update(
            Object sourceNode,
            @Nullable Object targetNode,
            @Nullable Map<String, Value> sourceNodePropertyValues,
            @Nullable Map<String, Value> targetNodePropertyValues,
            NodeLabelToken sourceNodeLabels,
            NodeLabelToken targetNodeLabels,
            @Nullable Map<String, Object> relationshipConfig
        ) {
            Map<String, Value> relationshipProperties = null;
            RelationshipType relationshipType = RelationshipType.ALL_RELATIONSHIPS;

            if (relationshipConfig != null) {
                relationshipProperties = propertiesConfig("properties", relationshipConfig);
                relationshipType = typeConfig("relationshipType", relationshipConfig);

                if (!relationshipConfig.isEmpty()) {
                    CypherMapWrapper.create(relationshipConfig).requireOnlyKeysFrom(List.of(
                        "properties",
                        "relationshipType"
                    ));
                }
            }

            var intermediateSourceId = loadNode(sourceNode, sourceNodeLabels, sourceNodePropertyValues);

            if (targetNode != null) {
                var relImporter = this.relImporters.computeIfAbsent(relationshipType, this::newRelImporter);
                var intermediateTargetId = loadNode(targetNode, targetNodeLabels, targetNodePropertyValues);

                if (this.relationshipPropertySchemas != null) {
                    assert relationshipProperties != null;
                    if (this.relationshipPropertySchemas.size() == 1) {
                        var relationshipProperty = this.relationshipPropertySchemas.get(0).key();
                        double propertyValue = loadOneRelationshipProperty(
                            relationshipProperties,
                            relationshipProperty
                        );
                        relImporter.addFromInternal(intermediateSourceId, intermediateTargetId, propertyValue);
                    } else {
                        var propertyValues = loadMultipleRelationshipProperties(
                            relationshipProperties,
                            this.relationshipPropertySchemas
                        );
                        relImporter.addFromInternal(intermediateSourceId, intermediateTargetId, propertyValues);
                    }
                } else {
                    relImporter.addFromInternal(intermediateSourceId, intermediateTargetId);
                }
            }
        }

        AggregationResult result(String username, DatabaseId databaseId, ProgressTimer timer) {

            var graphName = this.graphName;

            // in case something else has written something with the same graph name
            // validate again before doing the heavier graph building
            validateGraphName(graphName, username, databaseId);

            this.idMapBuilder.prepareForFlush();

            var graphStoreBuilder = new GraphStoreBuilder()
                .concurrency(this.config.readConcurrency())
                .capabilities(ImmutableStaticCapabilities.of(true))
                .databaseId(databaseId);

            var valueMapper = buildNodesWithProperties(graphStoreBuilder);
            buildRelationshipsWithProperties(graphStoreBuilder, valueMapper);

            var graphStore = graphStoreBuilder.schema(this.graphSchemaBuilder.build()).build();

            GraphStoreCatalog.set(this.config, graphStore);

            var projectMillis = timer.stop().getDuration();

            return AggregationResultImpl.builder()
                .graphName(graphName)
                .nodeCount(graphStore.nodeCount())
                .relationshipCount(graphStore.relationshipCount())
                .projectMillis(projectMillis)
                .configuration(this.config.toMap())
                .build();
        }

        private RelationshipsBuilder newRelImporter(RelationshipType relType) {

            var undirectedTypes = this.config.undirectedRelationshipTypes();
            var orientation = undirectedTypes.contains(relType.name) || undirectedTypes.contains("*")
                ? UNDIRECTED
                : NATURAL;

            var relationshipsBuilderBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(this.idMapBuilder)
                .orientation(orientation)
                .aggregation(Aggregation.NONE)
                .concurrency(this.config.readConcurrency());

            // There is a potential race between initializing the relationships builder and the
            // relationship property schemas. Both happen under lock, but under different ones.
            // Relationship builders are initialized as part of computeIfAbsent which uses the
            // lock inside ConcurrentHashMap, while `this.relationshipPropertySchemas` is initialized
            // using the lock in this class.
            //
            // We have to ensure that the property schemas field is fully initialized, before we
            // create the relationships builder. This can only be achieved by using the same lock
            // for both actions. This should not affect performance, as we are doing this inside of
            // computeIfAbsent which is only called once.
            this.lock.lock();
            try {
                if (this.relationshipPropertySchemas != null) {
                    for (var ignored : this.relationshipPropertySchemas) {
                        relationshipsBuilderBuilder.addPropertyConfig(
                            Aggregation.NONE,
                            DefaultValue.forDouble()
                        );
                    }
                }
            } finally {
                this.lock.unlock();
            }

            return relationshipsBuilderBuilder.build();
        }

        /**
         * Adds the given node to the internal nodes builder and returns
         * the intermediate node id which can be used for relationships.
         *
         * @return intermediate node id
         */
        private long loadNode(
            @Nullable Object node,
            NodeLabelToken nodeLabels,
            @Nullable Map<String, Value> nodeProperties
        ) {
            var originalNodeId = extractNodeId(node);

            return nodeProperties == null
                ? this.idMapBuilder.addNode(originalNodeId, nodeLabels)
                : this.idMapBuilder.addNodeWithProperties(
                    originalNodeId,
                    PropertyValues.of(nodeProperties),
                    nodeLabels
                );
        }

        private static double loadOneRelationshipProperty(
            @NotNull Map<String, Value> relationshipProperties,
            String relationshipPropertyKey
        ) {
            var propertyValueObject = relationshipProperties.get(relationshipPropertyKey);
            var propertyValue = Objects.requireNonNullElse(propertyValueObject, Values.NO_VALUE);
            return ReadHelper.extractValue(propertyValue, DefaultValue.DOUBLE_DEFAULT_FALLBACK);
        }

        private static double[] loadMultipleRelationshipProperties(
            @NotNull Map<String, Value> relationshipProperties,
            List<RelationshipPropertySchema> relationshipPropertyKeys
        ) {
            var propertyValues = new double[relationshipPropertyKeys.size()];
            Arrays.setAll(propertyValues, i -> {
                var relationshipPropertyKey = relationshipPropertyKeys.get(i).key();
                return loadOneRelationshipProperty(relationshipProperties, relationshipPropertyKey);
            });
            return propertyValues;
        }

        private AdjacencyCompressor.ValueMapper buildNodesWithProperties(GraphStoreBuilder graphStoreBuilder) {

            var idMapAndProperties = this.idMapBuilder.build();
            var nodes = idMapAndProperties.idMap();

            var maybeNodeProperties = idMapAndProperties.nodeProperties();

            graphStoreBuilder.nodes(nodes);

            var nodePropertySchema = maybeNodeProperties
                .map(nodeProperties -> nodeSchemaWithProperties(
                    nodes.availableNodeLabels(),
                    nodeProperties
                ))
                .orElseGet(() -> nodeSchemaWithoutProperties(nodes.availableNodeLabels()))
                .unionProperties();

            NodeSchema nodeSchema = NodeSchema.empty();
            nodes.availableNodeLabels().forEach(nodeSchema::getOrCreateLabel);
            nodePropertySchema.forEach((propertyKey, propertySchema) -> {
                nodes.availableNodeLabels().forEach(label -> {
                    nodeSchema.getOrCreateLabel(label).addProperty(propertySchema.key(), propertySchema);
                });
            });

            this.graphSchemaBuilder.nodeSchema(nodeSchema);

            maybeNodeProperties.ifPresent(allNodeProperties -> {
                CSRGraphStoreUtil.extractNodeProperties(
                    graphStoreBuilder,
                    nodePropertySchema::get,
                    allNodeProperties
                );
            });

            // Relationships are added using their intermediate node ids.
            // In order to map to the final internal ids, we need to use
            // the mapping function of the wrapped id map.
            return nodes.rootIdMap()::toMappedNodeId;
        }

        private static NodeSchema nodeSchemaWithProperties(
            Iterable<NodeLabel> nodeLabels,
            Map<String, NodePropertyValues> propertyMap
        ) {
            var nodeSchema = NodeSchema.empty();

            nodeLabels.forEach((nodeLabel) -> {
                propertyMap.forEach((propertyName, nodeProperties) -> {
                    nodeSchema.getOrCreateLabel(nodeLabel).addProperty(
                        propertyName,
                        nodeProperties.valueType()
                    );
                });
            });

            return nodeSchema;
        }

        private static NodeSchema nodeSchemaWithoutProperties(Iterable<NodeLabel> nodeLabels) {
            var nodeSchema = NodeSchema.empty();
            nodeLabels.forEach(nodeSchema::getOrCreateLabel);
            return nodeSchema;
        }

        private void buildRelationshipsWithProperties(
            GraphStoreBuilder graphStoreBuilder,
            AdjacencyCompressor.ValueMapper valueMapper
        ) {
            var relationshipSchema = RelationshipSchema.empty();

            var relationshipImportResultBuilder = RelationshipImportResult.builder();

            this.relImporters.forEach((relationshipType, relImporter) -> {
                var allRelationships = relImporter.buildAll(
                    Optional.of(valueMapper),
                    Optional.empty()
                );

                var firstRelationshipsAndDirection = allRelationships.get(0);
                var topology = firstRelationshipsAndDirection.relationships().topology();
                var direction = firstRelationshipsAndDirection.direction();

                var propertyStore = CSRGraphStoreUtil.buildRelationshipPropertyStore(
                    allRelationships,
                    Objects.requireNonNullElse(this.relationshipPropertySchemas, List.of())
                );

                var relationshipSchemaEntry = relationshipSchema.getOrCreateRelationshipType(relationshipType, direction);
                propertyStore.relationshipProperties().forEach((propertyKey, relationshipProperties) -> {
                    relationshipSchemaEntry
                        .addProperty(
                            propertyKey,
                            relationshipProperties.propertySchema()
                        );
                });

                relationshipImportResultBuilder.putImportResult(
                    relationshipType,
                    SingleTypeRelationshipImportResult.builder()
                        .topology(topology)
                        .properties(propertyStore)
                        .direction(Direction.DIRECTED)
                        .build()
                );
            });

            graphStoreBuilder.relationshipImportResult(relationshipImportResultBuilder.build());
            this.graphSchemaBuilder.relationshipSchema(relationshipSchema);

            // release all references to the builders
            // we are only be called once and don't support double invocations of `result` building
            this.relImporters.clear();
        }

        @Nullable
        private static Map<String, Value> propertiesConfig(
            String propertyKey,
            @NotNull Map<String, Object> propertiesConfig
        ) {
            var nodeProperties = propertiesConfig.remove(propertyKey);
            if (nodeProperties == null || nodeProperties instanceof Map) {
                //noinspection unchecked
                return objectsToValues((Map<String, Object>) nodeProperties);
            }
            throw new IllegalArgumentException(formatWithLocale(
                "The value of `%s` must be a `Map of Property Values`, but was `%s`.",
                propertyKey,
                nodeProperties.getClass().getSimpleName()
            ));
        }

        private static @Nullable Map<String, Value> objectsToValues(@Nullable Map<String, Object> properties) {
            if (properties == null) {
                return null;
            }
            var values = new HashMap<String, Value>(properties.size());
            properties.forEach((key, valueObject) -> {
                if (valueObject != null) {
                    var value = ValueConverter.toValue(valueObject);
                    values.put(key, value);
                }
            });
            return values;
        }

        private static RelationshipType typeConfig(
            @SuppressWarnings("SameParameterValue") String relationshipTypeKey,
            @NotNull Map<String, Object> relationshipConfig
        ) {
            var relationshipTypeEntry = relationshipConfig.remove(relationshipTypeKey);
            if (relationshipTypeEntry instanceof String) {
                return RelationshipType.of((String) relationshipTypeEntry);
            }
            if (relationshipTypeEntry == null) {
                return RelationshipType.ALL_RELATIONSHIPS;
            }
            throw new IllegalArgumentException(formatWithLocale(
                "The value of `%s` must be `String`, but was `%s`.",
                relationshipTypeKey,
                relationshipTypeEntry.getClass().getSimpleName()
            ));
        }

        private static long extractNodeId(@Nullable Object node) {
            if (node instanceof Node) {
                //noinspection removal
                return ((Node) node).getId();
            } else if (node instanceof Long) {
                return (Long) node;
            } else if (node instanceof Integer) {
                return (Integer) node;
            } else {
                throw invalidNodeType(node);
            }
        }

        private static IllegalArgumentException invalidNodeType(@Nullable Object node) {
            // According to the docs of @UserAggregation, possible types are:
            //   String
            //   Long or long
            //   Double or double
            //   Number
            //   Boolean or boolean
            //   org.neo4j.graphdb.Node
            //   org.neo4j.graphdb.Relationship
            //   org.neo4j.graphdb.Path
            //   java.util.Map with key String and value of any type in this list, including java.util.Map
            //   java.util.List with element type of any type in this list, including java.util.List

            String nodeType;
            if (node instanceof String) {
                nodeType = "STRING";
            } else if (node instanceof Number) {
                nodeType = "FLOAT";
            } else if (node instanceof Boolean) {
                nodeType = "BOOLEAN";
            } else if (node instanceof Relationship) {
                nodeType = "RELATIONSHIP";
            } else if (node instanceof Path) {
                nodeType = "PATH";
            } else if (node instanceof Map) {
                nodeType = "MAP";
            } else if (node instanceof List) {
                nodeType = "LIST";
            } else if (node == null) {
                nodeType = "NULL";
            } else {
                // should not happen unless new types are introduced into the procedure framework
                nodeType = "UNKNOWN: " + node.getClass().getName();
            }

            return new IllegalArgumentException("The node has to be either a NODE or an INTEGER, but got " + nodeType);
        }
    }


    @ValueClass
    @Configuration
    @SuppressWarnings("immutables:subtype")
    public interface GraphProjectFromCypherAggregationConfig extends GraphProjectConfig {

        @org.immutables.value.Value.Default
        @Configuration.Ignore
        default Orientation orientation() {
            return NATURAL;
        }

        @org.immutables.value.Value.Default
        @Configuration.Ignore
        default Aggregation aggregation() {
            return Aggregation.NONE;
        }

        @org.immutables.value.Value.Default
        default List<String> undirectedRelationshipTypes() {
            return List.of();
        }

        @Configuration.Ignore
        @Override
        default GraphStoreFactory.Supplier graphStoreFactory() {
            throw new UnsupportedOperationException(
                "Cypher aggregation does not work over the default graph store framework"
            );
        }

        @org.immutables.value.Value.Derived
        @Configuration.Ignore
        default Set<String> outputFieldDenylist() {
            return Set.of(
                NODE_COUNT_KEY,
                RELATIONSHIP_COUNT_KEY,
                READ_CONCURRENCY_KEY,
                SUDO_KEY,
                VALIDATE_RELATIONSHIPS_KEY
            );
        }

        static GraphProjectFromCypherAggregationConfig of(String userName, String graphName, @Nullable Map<String, Object> config) {
            return new GraphProjectFromCypherAggregationConfigImpl(userName, graphName, CypherMapWrapper.create(config));
        }

        @Override
        @Configuration.Ignore
        default <R> R accept(GraphProjectConfig.Cases<R> cases) {
            if (cases instanceof Cases) {
                return ((Cases<R>) cases).cypherAggregation(this);
            }
            return null;
        }

        interface Cases<R> extends GraphProjectConfig.Cases<R> {

            R cypherAggregation(GraphProjectFromCypherAggregationConfig cypherAggregationConfig);
        }

        interface Visitor extends Cases<Void> {

            @Override
            default Void cypherAggregation(GraphProjectFromCypherAggregationConfig cypherAggregationConfig) {
                visit(cypherAggregationConfig);
                return null;
            }

            default void visit(GraphProjectFromCypherAggregationConfig cypherAggregationConfig) {}
        }
    }

    @Configuration
    public interface AggregationResult {
        @ReturnType.Include
        String graphName();

        @ReturnType.Include
        long nodeCount();

        @ReturnType.Include
        long relationshipCount();

        @ReturnType.Include
        long projectMillis();

        @ReturnType.Include
        Map<String, Object> configuration();

        @Configuration.ToMap
        Map<String, Object> toMap();
    }
}
