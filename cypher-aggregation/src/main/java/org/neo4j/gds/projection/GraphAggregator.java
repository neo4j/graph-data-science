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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.CustomProcedure;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.ImmutableGraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.compat.CompatUserAggregator;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.ConfigKeyValidation;
import org.neo4j.gds.core.compress.AdjacencyCompressor;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.ImmutableNodeImportResult;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.loading.LazyIdMapBuilder;
import org.neo4j.gds.core.loading.ReadHelper;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.PropertyValues;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.Name;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

// public is required for the Cypher runtime
@SuppressWarnings("WeakerAccess")
public class GraphAggregator implements CompatUserAggregator {

    private final DatabaseId databaseId;
    private final String username;
    private final boolean canWriteToDatabase;

    private final ProgressTimer progressTimer;
    private final ConfigValidator configValidator;

    // Used for initializing the data and rel importers
    private final Lock lock;
    private volatile @Nullable GraphAggregator.GraphImporter importer;

    // #result() may be called twice, we cache the result of the first call to return it again in the second invocation
    private @Nullable AggregationResult result;

    GraphAggregator(
        DatabaseId databaseId,
        String username,
        boolean canWriteToDatabase
    ) {
        this.databaseId = databaseId;
        this.username = username;
        this.canWriteToDatabase = canWriteToDatabase;
        this.progressTimer = ProgressTimer.start();
        this.lock = new ReentrantLock();
        this.configValidator = new ConfigValidator();
    }

    // NOTE: keep in sync with `CypherAggregation.FUNCTION_NAME`
    @CustomProcedure("gds.alpha.graph.project")
    public AggregationResult procedureSyntax(
        @Name("graphName") TextValue graphName,
        @Name("sourceNode") AnyValue sourceNode,
        @Name("targetNode") AnyValue targetNode,
        @Name("nodesConfig") AnyValue nodesConfig,
        @Name("relationshipConfig") AnyValue relationshipConfig,
        @Name("configuration") AnyValue config
    ) {
        throw new UnsupportedOperationException("This method is only used to document the procedure syntax.");
    }

    @Override
    public void update(AnyValue[] input) throws ProcedureException {
        try {
            this.projectNextRelationship(
                (TextValue) input[0],
                input[1],
                input[2],
                input[3],
                input[4],
                input[5]
            );
        } catch (Exception e) {
            throw new ProcedureException(
                Status.Procedure.ProcedureCallFailed,
                e,
                "Failed to invoke function `%s`: Caused by: %s",
                CypherAggregation.FUNCTION_NAME,
                e
            );
        }
    }

    void projectNextRelationship(
        TextValue graphName,
        AnyValue sourceNode,
        AnyValue targetNode,
        AnyValue nodesConfig,
        AnyValue relationshipConfig,
        AnyValue config
    ) {
        @Nullable MapValue sourceNodePropertyValues = null;
        @Nullable MapValue targetNodePropertyValues = null;
        NodeLabelToken sourceNodeLabels = NodeLabelTokens.missing();
        NodeLabelToken targetNodeLabels = NodeLabelTokens.missing();

        if (nodesConfig instanceof MapValue) {
            sourceNodePropertyValues = GraphImporter.propertiesConfig("sourceNodeProperties", (MapValue) nodesConfig);
            sourceNodeLabels = labelsConfig("sourceNodeLabels", (MapValue) nodesConfig);

            if (targetNode != NoValue.NO_VALUE) {
                targetNodePropertyValues = GraphImporter.propertiesConfig(
                    "targetNodeProperties",
                    (MapValue) nodesConfig
                );
                targetNodeLabels = labelsConfig("targetNodeLabels", (MapValue) nodesConfig);
            }

            this.configValidator.validateNodesConfig((MapValue) nodesConfig);
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
            relationshipConfig,
            this.configValidator
        );
    }

    private GraphImporter initGraphData(
        TextValue graphName,
        AnyValue config,
        @Nullable MapValue sourceNodePropertyValues,
        @Nullable MapValue targetNodePropertyValues,
        NodeLabelToken sourceNodeLabels,
        NodeLabelToken targetNodeLabels,
        AnyValue relationshipConfig
    ) {
        var data = this.importer;
        if (data != null) {
            return data;
        }

        this.lock.lock();
        try {
            data = this.importer;
            if (data == null) {
                this.importer = data = GraphImporter.of(
                    graphName,
                    this.username,
                    this.databaseId,
                    config,
                    sourceNodePropertyValues,
                    targetNodePropertyValues,
                    sourceNodeLabels,
                    targetNodeLabels,
                    relationshipConfig,
                    this.canWriteToDatabase,
                    this.lock
                );
            }
            return data;
        } finally {
            this.lock.unlock();
        }
    }

    private static NodeLabelToken labelsConfig(String nodeLabelKey, @NotNull MapValue nodesConfig) {
        var nodeLabelsEntry = nodesConfig.get(nodeLabelKey);
        return tryLabelsConfig(nodeLabelsEntry, nodeLabelKey);
    }

    private static NodeLabelToken tryLabelsConfig(AnyValue nodeLabels, String nodeLabelKey) {
        var nodeLabelToken = nodeLabels.map(ReadNodeLabels.INSTANCE);

        if (nodeLabelToken.isInvalid()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The value of `%s` must be either a `List of Strings`, a `String`, or a `Boolean`, but was `%s`.",
                nodeLabelKey,
                nodeLabels.getTypeName()
            ));
        }

        return nodeLabelToken;
    }

    @Override
    public AnyValue result() throws ProcedureException {
        AggregationResult result;
        try {
            result = buildGraph();
        } catch (Exception e) {
            throw new ProcedureException(
                Status.Procedure.ProcedureCallFailed,
                e,
                "Failed to invoke function `%s`: Caused by: %s",
                CypherAggregation.FUNCTION_NAME,
                e
            );
        }

        if (result == null) {
            return Values.NO_VALUE;
        }

        var builder = new MapValueBuilder(6);
        builder.add("graphName", Values.stringValue(result.graphName()));
        builder.add("nodeCount", Values.longValue(result.nodeCount()));
        builder.add("relationshipCount", Values.longValue(result.relationshipCount()));
        builder.add("projectMillis", Values.longValue(result.projectMillis()));
        builder.add("configuration", ValueUtils.asAnyValue(result.configuration()));
        return builder.build();
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

    private static final class ConfigValidator {
        private static final Set<String> NODES_CONFIG_KEYS = Set.of(
            "sourceNodeProperties",
            "sourceNodeLabels",
            "targetNodeProperties",
            "targetNodeLabels"
        );

        private static final Set<String> RELATIONSHIPS_CONFIG_KEYS = Set.of(
            "properties",
            "relationshipType"
        );

        private final AtomicBoolean validateNodes = new AtomicBoolean(true);
        private final AtomicBoolean validateRelationships = new AtomicBoolean(true);

        void validateNodesConfig(MapValue nodesConfig) {
            if (this.validateNodes.getAndSet(false)) {
                ConfigKeyValidation.requireOnlyKeysFrom(NODES_CONFIG_KEYS, nodesConfig.keySet());
            }
        }

        void validateRelationshipsConfig(MapValue relationshipConfig) {
            if (this.validateRelationships.getAndSet(false)) {
                ConfigKeyValidation.requireOnlyKeysFrom(RELATIONSHIPS_CONFIG_KEYS, relationshipConfig.keySet());
            }
        }
    }

    // Does the actual importing work once we can initialize it with the first row
    @SuppressWarnings("CodeBlock2Expr")
    private static final class GraphImporter {
        private final String graphName;
        private final GraphProjectFromCypherAggregationConfig config;
        private final LazyIdMapBuilder idMapBuilder;
        private final @Nullable List<RelationshipPropertySchema> relationshipPropertySchemas;

        private final boolean canWriteToDatabase;
        private final ExtractNodeId extractNodeId;
        private final Lock lock;
        private final Map<RelationshipType, RelationshipsBuilder> relImporters;
        private final ImmutableGraphSchema.Builder graphSchemaBuilder;

        private GraphImporter(
            String graphName,
            GraphProjectFromCypherAggregationConfig config,
            LazyIdMapBuilder idMapBuilder,
            @Nullable List<RelationshipPropertySchema> relationshipPropertySchemas,
            boolean canWriteToDatabase,
            Lock lock
        ) {
            this.graphName = graphName;
            this.config = config;
            this.idMapBuilder = idMapBuilder;
            this.relationshipPropertySchemas = relationshipPropertySchemas;
            this.canWriteToDatabase = canWriteToDatabase;
            this.lock = lock;
            this.relImporters = new ConcurrentHashMap<>();
            this.graphSchemaBuilder = ImmutableGraphSchema.builder();
            this.extractNodeId = new ExtractNodeId();
        }

        static GraphImporter of(
            TextValue graphNameValue,
            String username,
            DatabaseId databaseId,
            AnyValue configMap,
            @Nullable MapValue sourceNodePropertyValues,
            @Nullable MapValue targetNodePropertyValues,
            NodeLabelToken sourceNodeLabels,
            NodeLabelToken targetNodeLabels,
            AnyValue relationshipConfig,
            boolean canWriteToDatabase,
            Lock lock
        ) {

            var graphName = graphNameValue.stringValue();

            validateGraphName(graphName, username, databaseId);
            var config = GraphProjectFromCypherAggregationConfig.of(
                username,
                graphName,
                (configMap instanceof MapValue) ? (MapValue) configMap : MapValue.EMPTY
            );

            var idMapBuilder = idMapBuilder(
                sourceNodeLabels,
                sourceNodePropertyValues,
                targetNodeLabels,
                targetNodePropertyValues,
                config.readConcurrency()
            );

            var relationshipPropertySchemas = relationshipPropertySchemas(relationshipConfig);

            return new GraphImporter(
                graphName,
                config,
                idMapBuilder,
                relationshipPropertySchemas,
                canWriteToDatabase,
                lock
            );
        }

        private static void validateGraphName(String graphName, String username, DatabaseId databaseId) {
            if (GraphStoreCatalog.exists(username, databaseId, graphName)) {
                throw new IllegalArgumentException("Graph " + graphName + " already exists");
            }
        }

        private static LazyIdMapBuilder idMapBuilder(
            NodeLabelToken sourceNodeLabels,
            @Nullable MapValue sourceNodeProperties,
            NodeLabelToken targetNodeLabels,
            @Nullable MapValue targetNodeProperties,
            int readConcurrency
        ) {
            boolean hasLabelInformation = !(sourceNodeLabels.isMissing() && targetNodeLabels.isMissing());
            boolean hasProperties = !(sourceNodeProperties == null && targetNodeProperties == null);
            return new LazyIdMapBuilder(readConcurrency, hasLabelInformation, hasProperties);
        }

        private static @Nullable List<RelationshipPropertySchema> relationshipPropertySchemas(AnyValue relationshipConfigValue) {
            if (!(relationshipConfigValue instanceof MapValue)) {
                return null;
            }

            //noinspection PatternVariableCanBeUsed
            var relationshipConfig = (MapValue) relationshipConfigValue;

            var relationshipPropertySchemas = new ArrayList<RelationshipPropertySchema>();

            // We need to do this before extracting the `relationshipProperties`, because
            // we remove the original entry from the map during converting; also we remove null keys
            // so we could not create a schema entry for properties that are absent on the current relationship
            var relationshipPropertyKeys = relationshipConfig.get("properties");
            if (relationshipPropertyKeys instanceof MapValue) {
                for (var propertyKey : ((MapValue) relationshipPropertyKeys).keySet()) {
                    relationshipPropertySchemas.add(RelationshipPropertySchema.of(
                        propertyKey,
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
            AnyValue sourceNode,
            AnyValue targetNode,
            @Nullable MapValue sourceNodePropertyValues,
            @Nullable MapValue targetNodePropertyValues,
            NodeLabelToken sourceNodeLabels,
            NodeLabelToken targetNodeLabels,
            AnyValue relationshipConfig,
            ConfigValidator configValidator
        ) {
            MapValue relationshipProperties = null;
            RelationshipType relationshipType = RelationshipType.ALL_RELATIONSHIPS;

            if (relationshipConfig instanceof MapValue) {
                relationshipProperties = propertiesConfig("properties", (MapValue) relationshipConfig);
                relationshipType = typeConfig("relationshipType", (MapValue) relationshipConfig);

                configValidator.validateRelationshipsConfig((MapValue) relationshipConfig);
            }

            var intermediateSourceId = loadNode(sourceNode, sourceNodeLabels, sourceNodePropertyValues);

            if (targetNode != NoValue.NO_VALUE) {
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

            var canWriteToDatabase = this.canWriteToDatabase && !this.extractNodeId.hasSeenArbitraryIds();

            var graphStoreBuilder = new GraphStoreBuilder()
                .concurrency(this.config.readConcurrency())
                .capabilities(ImmutableStaticCapabilities.of(canWriteToDatabase))
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

            List<String> inverseIndexedRelationshipTypes = this.config.inverseIndexedRelationshipTypes();
            boolean indexInverse = inverseIndexedRelationshipTypes.contains(relType.name)
                                   || inverseIndexedRelationshipTypes.contains("*");

            var relationshipsBuilderBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(this.idMapBuilder)
                .orientation(orientation)
                .aggregation(Aggregation.NONE)
                .indexInverse(indexInverse)
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
                    for (var relationshipPropertySchema : this.relationshipPropertySchemas) {
                        relationshipsBuilderBuilder.addPropertyConfig(
                            GraphFactory.PropertyConfig.of(relationshipPropertySchema.key())
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
            @NotNull AnyValue node,
            NodeLabelToken nodeLabels,
            @Nullable MapValue nodeProperties
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
            @NotNull MapValue relationshipProperties,
            String relationshipPropertyKey
        ) {
            var propertyValue = relationshipProperties.get(relationshipPropertyKey);
            return ReadHelper.extractValue(propertyValue, DefaultValue.DOUBLE_DEFAULT_FALLBACK);
        }

        private static double[] loadMultipleRelationshipProperties(
            @NotNull MapValue relationshipProperties,
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

            var nodesImportResultBuilder = ImmutableNodeImportResult.builder().idMap(nodes);

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
                    nodesImportResultBuilder,
                    nodePropertySchema::get,
                    allNodeProperties
                );
            });

            graphStoreBuilder.nodeImportResult(nodesImportResultBuilder.build());

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
            var relationshipImportResultBuilder = RelationshipImportResult.builder();
            var relationshipSchemas = new ArrayList<RelationshipSchema>();

            this.relImporters.forEach((relationshipType, relImporter) -> {
                var relationships = relImporter.build(
                    Optional.of(valueMapper),
                    Optional.empty()
                );
                relationshipSchemas.add(relationships.relationshipSchema(relationshipType));
                relationshipImportResultBuilder.putImportResult(relationshipType, relationships);
            });

            var relationshipSchema = relationshipSchemas
                .stream()
                .reduce(RelationshipSchema.empty(), RelationshipSchema::union);

            graphStoreBuilder.relationshipImportResult(relationshipImportResultBuilder.build());
            this.graphSchemaBuilder.relationshipSchema(relationshipSchema);

            // release all references to the builders
            // we are only be called once and don't support double invocations of `result` building
            this.relImporters.clear();
        }

        @Nullable
        static MapValue propertiesConfig(
            String propertyKey,
            @NotNull MapValue propertiesConfig
        ) {
            var nodeProperties = propertiesConfig.get(propertyKey);
            if (nodeProperties instanceof MapValue) {
                return (MapValue) nodeProperties;
            }
            if (nodeProperties == NoValue.NO_VALUE) {
                return null;
            }

            throw new IllegalArgumentException(formatWithLocale(
                "The value of `%s` must be a `Map of Property Values`, but was `%s`.",
                propertyKey,
                nodeProperties.getTypeName()
            ));
        }

        private static RelationshipType typeConfig(
            @SuppressWarnings("SameParameterValue") String relationshipTypeKey,
            @NotNull MapValue relationshipConfig
        ) {
            var relationshipTypeEntry = relationshipConfig.get(relationshipTypeKey);
            if (relationshipTypeEntry instanceof TextValue) {
                return RelationshipType.of(((TextValue) relationshipTypeEntry).stringValue());
            }
            if (relationshipTypeEntry == NoValue.NO_VALUE) {
                return RelationshipType.ALL_RELATIONSHIPS;
            }

            throw new IllegalArgumentException(formatWithLocale(
                "The value of `%s` must be `String`, but was `%s`.",
                relationshipTypeKey,
                relationshipTypeEntry.valueRepresentation().valueGroup()
            ));
        }

        private long extractNodeId(@NotNull AnyValue node) {
            return node.map(this.extractNodeId);
        }
    }
}
