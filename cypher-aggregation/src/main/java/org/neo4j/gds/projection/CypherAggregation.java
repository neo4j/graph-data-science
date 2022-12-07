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

import org.jetbrains.annotations.Contract;
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
import java.util.function.Supplier;

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
    @SuppressWarnings({"WeakerAccess", "CodeBlock2Expr"})
    public static class GraphAggregator {

        private final ProgressTimer progressTimer;
        private final DatabaseId databaseId;
        private final ImmutableGraphSchema.Builder graphSchemaBuilder;
        private final String username;

        // #result() is called twice, we cache the result of the first call to return it again in the second invocation
        private @Nullable AggregationResult result;
        private @Nullable String graphName;
        private @Nullable LazyIdMapBuilder idMapBuilder;
        private @Nullable List<RelationshipPropertySchema> relationshipPropertySchemas;
        private @Nullable GraphProjectFromCypherAggregationConfig config;

        private final Map<RelationshipType, RelationshipsBuilder> relImporters;
        // Used for initializing builders
        private final Lock lock;

        GraphAggregator(
            ProgressTimer progressTimer,
            DatabaseId databaseId,
            String username
        ) {
            this.progressTimer = progressTimer;
            this.databaseId = databaseId;
            this.username = username;
            this.relImporters = new ConcurrentHashMap<>();
            this.graphSchemaBuilder = ImmutableGraphSchema.builder();
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
            initGraphName(graphName);

            initConfig(config);

            Map<String, Value> sourceNodePropertyValues = null;
            Map<String, Value> targetNodePropertyValues = null;
            @Nullable NodeLabelToken sourceNodeLabels = null;
            @Nullable NodeLabelToken targetNodeLabels = null;

            if (nodesConfig != null) {
                sourceNodePropertyValues = propertiesConfig("sourceNodeProperties", nodesConfig);
                sourceNodeLabels = labelsConfig(sourceNode, "sourceNodeLabels", nodesConfig);

                if (targetNode != null) {
                    targetNodePropertyValues = propertiesConfig("targetNodeProperties", nodesConfig);
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

            initIdMapBuilder(sourceNodePropertyValues, targetNodePropertyValues, sourceNodeLabels, targetNodeLabels);

            Map<String, Value> relationshipProperties = null;
            RelationshipType relationshipType = RelationshipType.ALL_RELATIONSHIPS;

            if (relationshipConfig != null) {
                initRelationshipPropertySchemas(relationshipConfig);

                relationshipProperties = propertiesConfig("properties", relationshipConfig);
                relationshipType = typeConfig("relationshipType", relationshipConfig);

                if (!relationshipConfig.isEmpty()) {
                    CypherMapWrapper.create(relationshipConfig).requireOnlyKeysFrom(List.of(
                        "properties",
                        "relationshipType"
                    ));
                }
            }

            var relImporter = this.relImporters.computeIfAbsent(relationshipType, this::newRelImporter);

            var intermediateSourceId = loadNode(sourceNode, sourceNodeLabels, sourceNodePropertyValues);

            if (targetNode != null) {
                var intermediateTargetId = loadNode(targetNode, targetNodeLabels, targetNodePropertyValues);

                if (this.relationshipPropertySchemas != null && !this.relationshipPropertySchemas.isEmpty()) {
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

        private void initConfig(Map<String, Object> config) {
            if (this.config == null) {
                initObjectUnderLock(() -> this.config, () -> {
                    this.config = GraphProjectFromCypherAggregationConfig.of(username, Objects.requireNonNull(graphName), config);
                });
            }
        }

        private void initRelationshipPropertySchemas(@NotNull Map<String, Object> relationshipConfig) {
            if (this.relationshipPropertySchemas == null) {
                initObjectUnderLock(() -> this.relationshipPropertySchemas, () -> {
                    this.relationshipPropertySchemas = new ArrayList<>();

                    // We need to do this before extracting the `relationshipProperties`, because
                    // we remove the original entry from the map during converting; also we remove null keys
                    // so we could not create a schema entry for properties that are absent on the current relationship
                    var relationshipPropertyKeys = relationshipConfig.get("properties");
                    if (relationshipPropertyKeys instanceof Map) {
                        for (var propertyKey : ((Map<?, ?>) relationshipPropertyKeys).keySet()) {
                            this.relationshipPropertySchemas.add(RelationshipPropertySchema.of(
                                String.valueOf(propertyKey),
                                ValueType.DOUBLE
                            ));
                        }
                    }
                });
            }
        }

        private void initGraphName(String graphName) {
            if (this.graphName == null) {
                initObjectUnderLock(() -> this.graphName, () -> {
                    validateGraphName(graphName);
                    this.graphName = graphName;
                });
            }
        }

        private void initIdMapBuilder(
            Map<String, Value> sourceNodePropertyValues,
            Map<String, Value> targetNodePropertyValues,
            @Nullable NodeLabelToken sourceNodeLabels,
            @Nullable NodeLabelToken targetNodeLabels
        ) {
            if (this.idMapBuilder == null) {
                initObjectUnderLock(() -> this.idMapBuilder, () -> {
                    this.idMapBuilder = newIdMapBuilder(
                        sourceNodeLabels,
                        sourceNodePropertyValues,
                        targetNodeLabels,
                        targetNodePropertyValues
                    );
                });
            }
        }

        /**
         * Performs double-checked locking and executes the
         * given code which initializes the object.
         */
        private void initObjectUnderLock(Supplier<Object> s, Runnable code) {
            this.lock.lock();
            try {
                if (s.get() == null) {
                    code.run();
                }
            } finally {
                this.lock.unlock();
            }
        }

        @NotNull
        private LazyIdMapBuilder newIdMapBuilder(
            @Nullable NodeLabelToken sourceNodeLabels,
            @Nullable Map<String, Value> sourceNodeProperties,
            @Nullable NodeLabelToken targetNodeLabels,
            @Nullable Map<String, Value> targetNodeProperties
        ) {
            boolean hasLabelInformation = !(sourceNodeLabels == null && targetNodeLabels == null);
            boolean hasProperties = !(sourceNodeProperties == null && targetNodeProperties == null);
            return new LazyIdMapBuilder(config.readConcurrency(), hasLabelInformation, hasProperties);
        }

        private void validateGraphName(String graphName) {
            if (GraphStoreCatalog.exists(this.username, this.databaseId, graphName)) {
                throw new IllegalArgumentException("Graph " + graphName + " already exists");
            }
        }

        @Nullable
        private Map<String, Value> propertiesConfig(
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

        private @Nullable NodeLabelToken labelsConfig(
            Object node,
            String nodeLabelKey,
            @NotNull Map<String, Object> nodesConfig
        ) {
            var nodeLabelsEntry = nodesConfig.remove(nodeLabelKey);
            return tryLabelsConfig(node, nodeLabelsEntry, nodeLabelKey);
        }

        @Contract("_, null, _ -> null")
        private @Nullable NodeLabelToken tryLabelsConfig(
            Object node,
            @Nullable Object nodeLabels,
            String nodeLabelKey
        ) {
            if (nodeLabels == null || Boolean.FALSE.equals(nodeLabels)) {
                return null;
            }

            if (Boolean.TRUE.equals(nodeLabels)) {
                if (node instanceof Node) {
                    return NodeLabelTokens.ofNullable(((Node) node).getLabels());
                }
                throw new IllegalArgumentException(
                    "Using `true` to load all labels does only work if the node is a Neo4j node object"
                );
            }

            var nodeLabelToken = NodeLabelTokens.ofNullable(nodeLabels);

            if (nodeLabelToken == null) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The value of `%s` must be either a `List of Strings`, a `String`, or a `Boolean`, but was `%s`.",
                    nodeLabelKey,
                    nodeLabels.getClass().getSimpleName()
                ));
            }

            return nodeLabelToken;
        }

        private RelationshipType typeConfig(
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

        private RelationshipsBuilder newRelImporter(RelationshipType relType) {
            assert this.idMapBuilder != null;

            var undirectedTypes = config.undirectedRelationshipTypes();
            var orientation = undirectedTypes.contains(relType.name) || undirectedTypes.contains("*")
                ? UNDIRECTED
                : NATURAL;

            var relationshipsBuilderBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(this.idMapBuilder)
                .orientation(orientation)
                .aggregation(Aggregation.NONE)
                .concurrency(config.readConcurrency());

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

        private long extractNodeId(@Nullable Object node) {
            if (node instanceof Node) {
                return ((Node) node).getId();
            } else if (node instanceof Long) {
                return (Long) node;
            } else if (node instanceof Integer) {
                return (Integer) node;
            } else {
                throw invalidNodeType(node);
            }
        }

        private IllegalArgumentException invalidNodeType(@Nullable Object node) {
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

        /**
         * Adds the given node to the internal nodes builder and returns
         * the intermediate node id which can be used for relationships.
         *
         * @return intermediate node id
         */
        private long loadNode(
            @Nullable Object node,
            @Nullable NodeLabelToken nodeLabels,
            @Nullable Map<String, Value> nodeProperties
        ) {
            assert this.idMapBuilder != null;

            var originalNodeId = extractNodeId(node);

            return nodeProperties == null
                ? this.idMapBuilder.addNode(originalNodeId, nodeLabels)
                : this.idMapBuilder.addNodeWithProperties(originalNodeId, nodeProperties, nodeLabels);
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

        @UserAggregationResult
        @ReturnType(AggregationResult.class)
        public @Nullable Map<String, Object> result() {
            AggregationResult result = buildGraph();
            return result == null ? null : result.toMap();
        }

        public @Nullable AggregationResult buildGraph() {

            var graphName = this.graphName;

            if (graphName == null) {
                // Nothing aggregated
                return null;
            }

            if (this.result != null) {
                return this.result;
            }

            // in case something else has written something with the same graph name
            // validate again before doing the heavier graph building
            validateGraphName(graphName);

            var graphStoreBuilder = new GraphStoreBuilder()
                .concurrency(config.readConcurrency())
                .capabilities(ImmutableStaticCapabilities.of(true))
                .databaseId(databaseId);

            var valueMapper = buildNodesWithProperties(graphStoreBuilder);
            buildRelationshipsWithProperties(graphStoreBuilder, valueMapper);

            var graphStore = graphStoreBuilder.schema(graphSchemaBuilder.build()).build();

            GraphStoreCatalog.set(config, graphStore);

            var projectMillis = this.progressTimer.stop().getDuration();

            this.result = AggregationResultImpl.builder()
                .graphName(graphName)
                .nodeCount(graphStore.nodeCount())
                .relationshipCount(graphStore.relationshipCount())
                .projectMillis(projectMillis)
                .configuration(config.toMap())
                .build();

            return this.result;
        }

        private AdjacencyCompressor.ValueMapper buildNodesWithProperties(GraphStoreBuilder graphStoreBuilder) {
            assert this.idMapBuilder != null;

            var idMapAndProperties = this.idMapBuilder.build();
            var nodes = idMapAndProperties.idMap();

            var maybeNodeProperties = idMapAndProperties.nodeProperties();

            graphStoreBuilder.nodes(nodes);

            var nodePropertySchema = maybeNodeProperties
                .map(nodeProperties -> GraphAggregator.nodeSchemaWithProperties(
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

            graphSchemaBuilder.nodeSchema(nodeSchema);

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

                var topology = allRelationships.get(0).relationships().topology();

                var propertyStore = CSRGraphStoreUtil.buildRelationshipPropertyStore(
                    allRelationships,
                    Objects.requireNonNullElse(this.relationshipPropertySchemas, List.of())
                );

                propertyStore.relationshipProperties().forEach((propertyKey, relationshipProperties) -> {
                    relationshipSchema
                        .getOrCreateRelationshipType(relationshipType, Direction.DIRECTED)
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
            graphSchemaBuilder.relationshipSchema(relationshipSchema);

            // release all references to the builders
            // we are only be called once and don't support double invocations of `result` building
            this.relImporters.clear();
        }

        private static NodeSchema nodeSchemaWithProperties(Iterable<NodeLabel> nodeLabels, Map<String, NodePropertyValues> propertyMap) {
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
        @NotNull
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

        static GraphProjectFromCypherAggregationConfig of(String userName, String graphName, Map<String, Object> config) {
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
