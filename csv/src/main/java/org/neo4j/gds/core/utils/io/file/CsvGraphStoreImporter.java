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

import org.neo4j.common.Validator;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.logging.Log;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
    private final int concurrency;

    private final GraphStoreBuilder graphStoreBuilder;
    private final Log log;
    private final TaskRegistryFactory taskRegistryFactory;

    private ProgressTracker progressTracker;

    public static CsvGraphStoreImporter create(
        int concurrency,
        Path importPath,
        Log log,
        TaskRegistryFactory taskRegistryFactory
    ) {
        return new CsvGraphStoreImporter(
            new GraphStoreNodeVisitor.Builder(),
            new GraphStoreRelationshipVisitor.Builder(),
            concurrency,
            importPath,
            log,
            taskRegistryFactory
        );
    }

    private CsvGraphStoreImporter(
        GraphStoreNodeVisitor.Builder nodeVisitorBuilder,
        GraphStoreRelationshipVisitor.Builder relationshipVisitorBuilder,
        int concurrency,
        Path importPath,
        Log log,
        TaskRegistryFactory taskRegistryFactory
    ) {
        this.nodeVisitorBuilder = nodeVisitorBuilder;
        this.relationshipVisitorBuilder = relationshipVisitorBuilder;
        this.concurrency = concurrency;
        this.importPath = importPath;
        this.graphStoreBuilder = new GraphStoreBuilder().concurrency(concurrency);
        this.log = log;
        this.taskRegistryFactory = taskRegistryFactory;
    }

    public UserGraphStore run() {
        var fileInput = new FileInput(importPath);
        this.progressTracker = createProgressTracker(fileInput);
        progressTracker.beginSubTask();
        try {
            importGraph(fileInput);
            return ImmutableUserGraphStore.of(fileInput.userName(), graphStoreBuilder.build());
        } finally {
            progressTracker.endSubTask();
        }
    }

    private ProgressTracker createProgressTracker(FileInput fileInput) {
        var graphInfo = fileInput.graphInfo();
        var nodeCount = graphInfo.nodeCount();

        var importTasks = new ArrayList<Task>();
        importTasks.add(Tasks.leaf("Import nodes", nodeCount));

        var relationshipTaskVolume = graphInfo.relationshipTypeCounts().isEmpty()
            ? Task.UNKNOWN_VOLUME
            : graphInfo.relationshipTypeCounts().values().stream().mapToLong(Long::longValue).sum();
        importTasks.add(Tasks.leaf("Import relationships", relationshipTaskVolume));

        var task = Tasks.task(
            "Csv import",
            importTasks
        );

        return new TaskProgressTracker(task, log, concurrency, taskRegistryFactory);
    }

    private void importGraph(FileInput fileInput) {
        var nodes = importNodes(fileInput);
        importRelationships(fileInput, nodes);
    }

    private IdMap importNodes(
        FileInput fileInput
    ) {
        progressTracker.beginSubTask();
        NodeSchema nodeSchema = fileInput.nodeSchema();

        GraphInfo graphInfo = fileInput.graphInfo();
        graphStoreBuilder.databaseId(graphInfo.namedDatabaseId());

        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder(nodeSchema)
            .maxOriginalId(graphInfo.maxOriginalId())
            .concurrency(concurrency)
            .nodeCount(graphInfo.nodeCount())
            .deduplicateIds(false)
            .build();
        nodeVisitorBuilder.withNodeSchema(nodeSchema);
        nodeVisitorBuilder.withNodesBuilder(nodesBuilder);

        var nodesIterator = fileInput.nodes(Collector.EMPTY).iterator();
        Collection<Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            (index) -> new ElementImportRunner(nodeVisitorBuilder.build(), nodesIterator, progressTracker)
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var idMapAndProperties = nodesBuilder.build();
        graphStoreBuilder.nodes(idMapAndProperties.idMap());
        var schemaProperties = nodeSchema.properties();
        CSRGraphStoreUtil.extractNodeProperties(
            graphStoreBuilder,
            schemaProperties::get,
            idMapAndProperties.nodeProperties().orElse(Map.of())
        );

        progressTracker.endSubTask();
        return idMapAndProperties.idMap();
    }

    private void importRelationships(FileInput fileInput, IdMap nodes) {
        progressTracker.beginSubTask();
        ConcurrentHashMap<String, RelationshipsBuilder> relationshipBuildersByType = new ConcurrentHashMap<>();
        var relationshipSchema = fileInput.relationshipSchema();
        this.relationshipVisitorBuilder
            .withRelationshipSchema(relationshipSchema)
            .withNodes(nodes)
            .withConcurrency(concurrency)
            .withAllocationTracker()
            .withRelationshipBuildersToTypeResultMap(relationshipBuildersByType);

        var relationshipsIterator = fileInput.relationships(Collector.EMPTY).iterator();
        Collection<Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            (index) -> new ElementImportRunner(relationshipVisitorBuilder.build(), relationshipsIterator, progressTracker)
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var relationships = relationshipTopologyAndProperties(relationshipBuildersByType, relationshipSchema);

        graphStoreBuilder.relationships(relationships.topologies());
        graphStoreBuilder.relationshipPropertyStores(relationships.properties());

        progressTracker.endSubTask();
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
        var propertyStore = CSRGraphStoreUtil.buildRelationshipPropertyStore(relationships, propertySchemas);
        propertyStores.put(relationshipType, propertyStore);
        return relationships.get(0).topology();
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
