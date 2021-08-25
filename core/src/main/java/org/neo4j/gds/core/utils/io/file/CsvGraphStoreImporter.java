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
package org.neo4j.gds.core.utils.io.file;

import org.immutables.builder.Builder;
import org.neo4j.common.Validator;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ImmutableNodePropertyStore;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.NodeProperty;
import org.neo4j.gds.api.NodePropertyStore;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.beta.filter.GraphStoreFilter;
import org.neo4j.gds.beta.filter.expression.SemanticErrors;
import org.neo4j.gds.config.ImmutableGraphCreateFromGraphConfig;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.io.GraphStoreExporter;
import org.neo4j.gds.core.utils.io.ImmutableImportedProperties;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.NumberType;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class CsvGraphStoreImporter {

    private final GraphStoreNodeVisitor.Builder nodeVisitorBuilder;
    private final Path importPath;
    private final GraphStoreRelationshipVisitor.Builder relationshipVisitorBuilder;
    private final CsvGraphStoreImporterConfig config;

    private final GraphStoreBuilder graphStoreBuilder;
    private String userName;

    private final Log log;

    public static CsvGraphStoreImporter create(
        CsvGraphStoreImporterConfig config,
        Path importPath,
        Log log
    ) {
        return new CsvGraphStoreImporter(
            new GraphStoreNodeVisitor.Builder(),
            new GraphStoreRelationshipVisitor.Builder(),
            config,
            importPath,
            log
        );
    }

    private CsvGraphStoreImporter(
        GraphStoreNodeVisitor.Builder nodeVisitorBuilder,
        GraphStoreRelationshipVisitor.Builder relationshipVisitorBuilder,
        CsvGraphStoreImporterConfig config,
        Path importPath,
        Log log
    ) {
        this.nodeVisitorBuilder = nodeVisitorBuilder;
        this.relationshipVisitorBuilder = relationshipVisitorBuilder;
        this.config = config;
        this.importPath = importPath;
        this.graphStoreBuilder = new GraphStoreBuilder().concurrency(config.concurrency());
        this.log = log;
    }

    public UserGraphStore userGraphStore() {
        return ImmutableUserGraphStore.of(userName, graphStoreBuilder.build());
    }

    public GraphStoreExporter.ImportedProperties run(AllocationTracker tracker) {
        var fileInput = new FileInput(importPath);
        graphStoreBuilder.tracker(tracker);
        graphStoreBuilder.log(log);
        this.userName = fileInput.userName();
        return importGraph(fileInput, tracker);
    }

    private GraphStoreExporter.ImportedProperties importGraph(FileInput fileInput, AllocationTracker tracker) {
        var nodes = importNodes(fileInput, tracker);
        var importedRelationships = importRelationships(fileInput, nodes, AllocationTracker.empty());
        return ImmutableImportedProperties.of(nodes.nodeCount(), importedRelationships);
    }

    private NodeMapping importNodes(
        FileInput fileInput,
        AllocationTracker tracker
    ) {
        NodeSchema nodeSchema = fileInput.nodeSchema();

        GraphInfo graphInfo = fileInput.graphInfo();
        graphStoreBuilder.useBitIdMap(graphInfo.bitIdMap());
        graphStoreBuilder.databaseId(graphInfo.namedDatabaseId());

        int concurrency = config.concurrency();
        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder(nodeSchema)
            .maxOriginalId(graphInfo.maxOriginalId())
            .concurrency(concurrency)
            .nodeCount(graphInfo.nodeCount())
            .tracker(tracker)
            .build();
        nodeVisitorBuilder.withNodeSchema(nodeSchema);
        nodeVisitorBuilder.withNodesBuilder(nodesBuilder);

        var nodesIterator = fileInput.nodes(Collector.EMPTY).iterator();
        Collection<Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            (index) -> new ElementImportRunner(nodeVisitorBuilder.build(), nodesIterator)
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var nodeMappingAndProperties = nodesBuilder.build();
        graphStoreBuilder.nodes(nodeMappingAndProperties.nodeMapping());
        nodeMappingAndProperties.nodeProperties().orElse(Map.of())
            .forEach((label, propertyMap) -> {
                var nodeStoreProperties = propertyKeyToNodePropertyMapping(nodeSchema, label, propertyMap);
                graphStoreBuilder.putNodePropertyStores(label, ImmutableNodePropertyStore.of(nodeStoreProperties));
            });

        return nodeMappingAndProperties.nodeMapping();
    }

    private Map<String, NodeProperty> propertyKeyToNodePropertyMapping(
        NodeSchema nodeSchema,
        NodeLabel label,
        Map<String, NodeProperties> propertyMap
    ) {
        Map<String, PropertySchema> propertySchemaForLabel = nodeSchema.properties().get(label);
        return propertyMap.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> nodePropertiesFrom(entry.getKey(), entry.getValue(), propertySchemaForLabel)
            ));
    }

    private NodeProperty nodePropertiesFrom(String propertyKey, NodeProperties nodeProperties, Map<String, PropertySchema> propertySchema) {
        var propertySchemaForKey = propertySchema.get(propertyKey);
        return NodeProperty.of(
            propertySchemaForKey.key(),
            propertySchemaForKey.state(),
            nodeProperties,
            propertySchemaForKey.defaultValue()
        );
    }

    private long importRelationships(FileInput fileInput, NodeMapping nodes, AllocationTracker tracker) {
        int concurrency = config.concurrency();

        ConcurrentHashMap<String, RelationshipsBuilder> relationshipBuildersByType = new ConcurrentHashMap<>();
        var relationshipSchema = fileInput.relationshipSchema();
        this.relationshipVisitorBuilder
            .withRelationshipSchema(relationshipSchema)
            .withNodes(nodes)
            .withConcurrency(concurrency)
            .withAllocationTracker(tracker)
            .withRelationshipBuildersToTypeResultMap(relationshipBuildersByType);

        var relationshipsIterator = fileInput.relationships(Collector.EMPTY).iterator();
        Collection<Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            (index) -> new ElementImportRunner(relationshipVisitorBuilder.build(), relationshipsIterator)
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var relationships = relationshipTopologyAndProperties(relationshipBuildersByType, relationshipSchema);

        graphStoreBuilder.relationships(relationships.topologies());
        graphStoreBuilder.relationshipPropertyStores(relationships.properties());
        return relationships.importedRelationships();
    }

    static RelationshipTopologyAndProperties relationshipTopologyAndProperties(
        Map<String, RelationshipsBuilder> relationshipBuildersByType,
        RelationshipSchema relationshipSchema
    ) {
        var propertyStores = new HashMap<RelationshipType, RelationshipPropertyStore>();
        var relationshipTypeTopologyMap = relationshipTypeToTopologyMapping(
            relationshipBuildersByType,
            propertyStores,
            relationshipSchema
        );

        var importedRelationships = relationshipTypeTopologyMap.values().stream().mapToLong(Relationships.Topology::elementCount).sum();
        return ImmutableRelationshipTopologyAndProperties.of(relationshipTypeTopologyMap, propertyStores, importedRelationships);
    }

    private static Map<RelationshipType, Relationships.Topology> relationshipTypeToTopologyMapping(
        Map<String, RelationshipsBuilder> relationshipBuildersByType,
        Map<RelationshipType, RelationshipPropertyStore> propertyStores,
        RelationshipSchema relationshipSchema
    ) {
        return relationshipBuildersByType.entrySet().stream().map(entry -> {
            var relationshipType = RelationshipType.of(entry.getKey());
            return Map.entry(
                relationshipType,
                relationshipTopologyFrom(
                    relationshipType,
                    entry.getValue().buildAll(),
                    relationshipSchema.propertySchemasFor(relationshipType),
                    propertyStores
                )
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Relationships.Topology relationshipTopologyFrom(
        RelationshipType relationshipType,
        List<Relationships> relationships,
        List<RelationshipPropertySchema> propertySchemas,
        Map<RelationshipType, RelationshipPropertyStore> propertyStores
    ) {
        var propertyStoreBuilder = RelationshipPropertyStore.builder();

        buildPropertyStores(
            relationships,
            propertyStoreBuilder,
            propertySchemas
        );

        propertyStores.put(relationshipType, propertyStoreBuilder.build());
        return relationships.get(0).topology();
    }

    private static void buildPropertyStores(
        java.util.List<Relationships> relationships,
        RelationshipPropertyStore.Builder propertyStoreBuilder,
        java.util.List<RelationshipPropertySchema> relationshipPropertySchemas
    ) {
        for (int i = 0; i < relationshipPropertySchemas.size(); i++) {
            var relationship = relationships.get(i);
            var relationshipPropertySchema = relationshipPropertySchemas.get(i);
            relationship.properties().ifPresent(properties -> {

                propertyStoreBuilder.putIfAbsent(relationshipPropertySchema.key(), RelationshipProperty.of(
                    relationshipPropertySchema.key(),
                    NumberType.FLOATING_POINT,
                    relationshipPropertySchema.state(),
                    properties,
                    relationshipPropertySchema.defaultValue(),
                    relationshipPropertySchema.aggregation()
                    )
                );
            });
        }
    }

    @ValueClass
    public interface UserGraphStore {
        String userName();
        GraphStore graphStore();
    }

    @ValueClass
    interface RelationshipTopologyAndProperties {
        Map<RelationshipType, Relationships.Topology> topologies();
        Map<RelationshipType, RelationshipPropertyStore> properties();
        long importedRelationships();
    }

    @Builder.Factory
    static GraphStore graphStore(
        NamedDatabaseId databaseId,
        NodeMapping nodes,
        Map<NodeLabel, NodePropertyStore> nodePropertyStores,
        Map<RelationshipType, Relationships.Topology> relationships,
        Map<RelationshipType, RelationshipPropertyStore> relationshipPropertyStores,
        int concurrency,
        boolean useBitIdMap,
        Log log,
        AllocationTracker tracker
    ) {
        var graphStore = CSRGraphStore.of(
            databaseId,
            nodes,
            nodePropertyStores,
            relationships,
            relationshipPropertyStores,
            concurrency,
            tracker
        );
        if (useBitIdMap) {
            // When the originally persisted graph was using a BitIdMap
            // we at first load an ordinary IdMap and transform it to a
            // BitIdMap afterwards. As the Graph filtering code path does
            // exactly that we call filtering with a true expression for
            // nodes and relationships.
            var config = ImmutableGraphCreateFromGraphConfig.builder()
                .concurrency(concurrency)
                .nodeFilter("*")
                .relationshipFilter("*")
                .graphName("new")
                .fromGraphName("old")
                .originalConfig(GraphCreateFromStoreConfig.emptyWithName("user", "old"))
                .build();
            try {
                return GraphStoreFilter.filter(
                    graphStore,
                    config,
                    Pools.DEFAULT,
                    log,
                    tracker,
                    EmptyProgressEventTracker.INSTANCE
                );
            } catch (ParseException | SemanticErrors e) {
                throw new RuntimeException(e);
            }
        }
        return graphStore;
    }

    public static final Validator<Path> DIRECTORY_IS_READABLE = value -> {
        Files.exists(value);
        if (!Files.isDirectory(value)) {
            throw new IllegalArgumentException("'" + value + "' is not a directory");
        }
        if (!Files.isReadable(value)) {
            throw new IllegalArgumentException("Directory '" + value + "' not readable");
        }
    };
}
