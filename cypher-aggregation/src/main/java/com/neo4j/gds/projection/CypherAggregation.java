/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package com.neo4j.gds.projection;

import com.carrotsearch.hppc.DoubleArrayList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;
import org.neo4j.gds.core.loading.ReadHelper;
import org.neo4j.gds.core.loading.ValueConverter;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongPredicate;

public final class CypherAggregation extends BaseProc {

    @UserAggregationFunction(name = "gds.alpha.graph")
    public GraphAggregator projectFromCypherAggregation() {

        var progressTimer = ProgressTimer.start();
        var catalogRequest = ImmutableCatalogRequest.of(
            databaseId().name(),
            username(),
            Optional.empty(),
            isGdsAdmin()
        );

        return new GraphAggregator(
            progressTimer,
            catalogRequest,
            this.api.databaseId(),
            username(),
            this.allocationTracker
        );
    }

    public static class GraphAggregator {

        private final ProgressTimer progressTimer;
        private final CatalogRequest catalogRequest;
        private final NamedDatabaseId databaseId;
        private final String username;
        private final AllocationTracker allocationTracker;

        private @Nullable String graphName;
        private @Nullable RelationshipsBuilder relImporter;
        private @Nullable NodeSchema nodeSchema;
        private @Nullable LazyIdMapBuilder idMapBuilder;
        private final List<RelationshipPropertySchema> relationshipPropertySchemas;

        GraphAggregator(
            ProgressTimer progressTimer,
            CatalogRequest catalogRequest,
            NamedDatabaseId databaseId,
            String username,
            AllocationTracker allocationTracker
        ) {
            this.progressTimer = progressTimer;
            this.catalogRequest = catalogRequest;
            this.databaseId = databaseId;
            this.username = username;
            this.allocationTracker = allocationTracker;
            this.relationshipPropertySchemas = new ArrayList<>();
        }

        @UserAggregationUpdate
        public void update(
            @Name("graphName") String graphName,
            @Name("sourceNode") Node sourceNode,
            @Nullable @Name("target") Node targetNode,
            @Nullable @Name(value = "sourceNodeProperties", defaultValue = "null") Map<String, Object> sourceNodeProperties,
            @Nullable @Name(value = "targetNodeProperties", defaultValue = "null") Map<String, Object> targetNodeProperties,
            @Nullable @Name(value = "relationshipProperties", defaultValue = "null") Map<String, Object> relationshipProperties
        ) {

            if (this.graphName == null) {
                validateGraphName(graphName);
                this.graphName = graphName;
            }

            var sourceNodePropertyValues = objectsToValues(sourceNodeProperties);
            var targetNodePropertyValues = objectsToValues(targetNodeProperties);

            if (this.nodeSchema == null && sourceNodePropertyValues != null) {
                this.nodeSchema = newNodeSchema(sourceNodePropertyValues);
            }

            if (this.idMapBuilder == null) {
                this.idMapBuilder = new LazyIdMapBuilder(this.nodeSchema, this.allocationTracker);
            }

            if (this.relImporter == null) {
                this.relImporter = newRelImporter(relationshipProperties);
            }

            var sourceNodeId = loadNode(sourceNode, sourceNodePropertyValues);

            if (targetNode != null) {
                var targetNodeId = loadNode(targetNode, targetNodePropertyValues);

                if (relationshipProperties != null) {
                    if (relationshipProperties.size() == 1) {
                        double propertyValue = loadOneRelationshipProperty(relationshipProperties);
                        this.relImporter.addFromInternal(sourceNodeId, targetNodeId, propertyValue);
                    } else {
                        var propertyValues = loadMultipleRelationshipProperties(relationshipProperties);
                        this.relImporter.addFromInternal(sourceNodeId, targetNodeId, propertyValues);
                    }
                } else {
                    this.relImporter.addFromInternal(sourceNodeId, targetNodeId);
                }
            }
        }

        private void validateGraphName(String graphName) {
            if (GraphStoreCatalog.exists(this.username, this.databaseId, graphName)) {
                throw new IllegalArgumentException("Graph " + graphName + " already exists");
            }
        }

        private RelationshipsBuilder newRelImporter(@Nullable Map<String, Object> relationshipProperties) {
            assert this.idMapBuilder != null;

            var relationshipsBuilderBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(this.idMapBuilder)
                .orientation(Orientation.NATURAL)
                .aggregation(Aggregation.NONE)
                // TODO: concurrency from config
                .concurrency(4)
                .allocationTracker(this.allocationTracker);

            if (relationshipProperties != null) {
                relationshipProperties.forEach((propertyKey, propertyValue) -> {
                    relationshipsBuilderBuilder.addPropertyConfig(
                        Aggregation.NONE,
                        DefaultValue.forDouble()
                    );
                    relationshipPropertySchemas.add(RelationshipPropertySchema.of(propertyKey, ValueType.DOUBLE));
                });
            }
            return relationshipsBuilderBuilder.build();
        }

        private NodeSchema newNodeSchema(Map<String, Value> nodeProperties) {
            var nodeSchemaBuilder = NodeSchema.builder();

            nodeProperties.forEach((propertyName, propertyValue) -> {
                nodeSchemaBuilder.addProperty(
                    NodeLabel.ALL_NODES,
                    propertyName,
                    ValueConverter.valueType(propertyValue)
                );
            });

            return nodeSchemaBuilder.build();
        }

        private static @Nullable Map<String, Value> objectsToValues(@Nullable Map<String, Object> properties) {
            if (properties == null) {
                return null;
            }
            var values = new HashMap<String, Value>(properties.size());
            properties.forEach((key, valueObject) -> {
                var value = ValueConverter.toValue(valueObject);
                values.put(key, value);
            });
            return values;
        }

        private long loadNode(Entity node, @Nullable Map<String, Value> nodeProperties) {
            return (nodeProperties == null)
                ? this.idMapBuilder.addNode(node.getId())
                : this.idMapBuilder.addNodeWithProperties(node.getId(), nodeProperties);
        }

        private double loadOneRelationshipProperty(@NotNull Map<String, Object> relationshipProperties) {
            var propertyValueObject = relationshipProperties.values().iterator().next();
            return ReadHelper.extractValue(
                ValueConverter.toValue(propertyValueObject),
                DefaultValue.DOUBLE_DEFAULT_FALLBACK
            );
        }

        private double[] loadMultipleRelationshipProperties(@NotNull Map<String, Object> relationshipProperties) {
            var propertyValues = new DoubleArrayList(relationshipProperties.size());
            relationshipProperties.forEach((propertyKey, propertyValueObject) -> {
                var propertyValue = ReadHelper.extractValue(
                    ValueConverter.toValue(propertyValueObject),
                    DefaultValue.DOUBLE_DEFAULT_FALLBACK
                );
                propertyValues.add(propertyValue);
            });
            return propertyValues.toArray();
        }

        @UserAggregationResult
        public Map<String, Object> result() {

            var graphName = this.graphName;

            if (graphName == null) {
                return Map.of(
                    "graphName", "<no data was projected>",
                    "nodeCount", 0,
                    "relationshipCount", 0,
                    "projectMillis", this.progressTimer.stop().getDuration()
                );
            }

            if (GraphStoreCatalog.exists(this.username, this.databaseId, graphName)) {
                var graphStore = GraphStoreCatalog.get(this.catalogRequest, graphName).graphStore();
                return Map.of(
                    "graphName", graphName,
                    "nodeCount", graphStore.nodeCount(),
                    "relationshipCount", graphStore.relationshipCount(),
                    "projectMillis", this.progressTimer.stop().getDuration()
                );
            }

            var graphStoreBuilder = new GraphStoreBuilder()
                .allocationTracker(allocationTracker)
                .concurrency(4)
                .databaseId(databaseId);

            assert this.idMapBuilder != null;

            var nodes = this.idMapBuilder.build();
            graphStoreBuilder.nodes(nodes.idMap());

            if (this.nodeSchema != null) {
                nodes.nodeProperties().ifPresent(allNodeProperties -> {
                    CSRGraphStoreUtil.extractNodeProperties(
                        graphStoreBuilder,
                        this.nodeSchema,
                        allNodeProperties
                    );
                });
            }

            assert this.relImporter != null;

            var allRelationships = this.relImporter.buildAll(Optional.of(nodes.idMap()::toMappedNodeId));
            var propertyStore = CSRGraphStoreUtil.buildRelationshipPropertyStore(
                allRelationships,
                this.relationshipPropertySchemas
            );

            graphStoreBuilder.putRelationships(RelationshipType.ALL_RELATIONSHIPS, allRelationships.get(0).topology());
            graphStoreBuilder.putRelationshipPropertyStores(RelationshipType.ALL_RELATIONSHIPS, propertyStore);

            var graphStore = graphStoreBuilder.build();

            var config = ImmutableGraphProjectFromCypherAggregation.builder()
                .graphName(graphName)
                .username(this.username)
                .build();

            GraphStoreCatalog.set(config, graphStore);

            var projectMillis = this.progressTimer.stop().getDuration();

            return Map.of(
                "graphName", graphName,
                "nodeCount", graphStore.nodeCount(),
                "relationshipCount", graphStore.relationshipCount(),
                "projectMillis", projectMillis
            );
        }
    }

    @ValueClass
    @SuppressWarnings("immutables:subtype")
    public interface GraphProjectFromCypherAggregation extends GraphProjectConfig {

        @org.immutables.value.Value.Default
        default Orientation orientation() {
            return Orientation.NATURAL;
        }

        @org.immutables.value.Value.Default
        default Aggregation aggregation() {
            return Aggregation.NONE;
        }

        @Override
        default GraphStoreFactory.Supplier graphStoreFactory() {
            throw new UnsupportedOperationException(
                "Cypher aggregation does not work over the default graph store framework"
            );
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

            R cypherAggregation(GraphProjectFromCypherAggregation cypherAggregationConfig);
        }
    }
}

final class LazyIdMapBuilder implements IdMap {
    private final NodesBuilder nodesBuilder;

    LazyIdMapBuilder(@Nullable NodeSchema nodeSchema, AllocationTracker allocationTracker) {
        this.nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(NodesBuilder.UNKNOWN_MAX_ID)
            .nodeSchema(Optional.ofNullable(nodeSchema))
            .deduplicateIds(true)
            .allocationTracker(allocationTracker)
            .build();
    }

    long addNode(long nodeId) {
        return addNodeWithProperties(nodeId, Map.of());
    }

    long addNodeWithProperties(long nodeId, Map<String, Value> properties) {
        if (properties.isEmpty()) {
            this.nodesBuilder.addNode(nodeId);
        } else {
            this.nodesBuilder.addNode(nodeId, properties);
        }
        return nodeId;
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return this.addNode(nodeId);
    }


    // TODO: throw unsupported operation instead of dummy impls
    @Override
    public long toOriginalNodeId(long nodeId) {
        return -1L;
    }

    @Override
    public long toRootNodeId(long nodeId) {
        return nodeId;
    }

    @Override
    public boolean contains(long nodeId) {
        return true;
    }

    @Override
    public long nodeCount() {
        return this.nodesBuilder.importedNodes();
    }

    @Override
    public long rootNodeCount() {
        return this.nodeCount();
    }

    @Override
    public long highestNeoId() {
        return -1L;
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(long batchSize) {
        return Set.of();
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        return Set.of();
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return Set.of();
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        return false;
    }

    @Override
    public IdMap rootIdMap() {
        return this;
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return new PrimitiveLongIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public long next() {
                return 0;
            }
        };
    }

    NodesBuilder.IdMapAndProperties build() {
        return this.nodesBuilder.build();
    }
}
