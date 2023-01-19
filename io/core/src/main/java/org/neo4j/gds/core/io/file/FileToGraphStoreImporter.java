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
package org.neo4j.gds.core.io.file;

import org.neo4j.common.Validator;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Topology;
import org.neo4j.gds.api.schema.ImmutableGraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.io.GraphStoreGraphPropertyVisitor;
import org.neo4j.gds.core.io.GraphStoreRelationshipVisitor;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.ImmutableNodes;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.loading.RelationshipImportResult;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class FileToGraphStoreImporter {

    private final GraphStoreNodeVisitor.Builder nodeVisitorBuilder;
    private final GraphStoreRelationshipVisitor.Builder relationshipVisitorBuilder;
    private final GraphStoreGraphPropertyVisitor.Builder graphPropertyVisitorBuilder;
    private final Path importPath;
    private final int concurrency;

    private final ImmutableGraphSchema.Builder graphSchemaBuilder;
    private final GraphStoreBuilder graphStoreBuilder;
    private final Log log;
    private final TaskRegistryFactory taskRegistryFactory;

    private ProgressTracker progressTracker;

    protected FileToGraphStoreImporter(
        int concurrency,
        Path importPath,
        Log log,
        TaskRegistryFactory taskRegistryFactory
    ) {
        this.nodeVisitorBuilder = new GraphStoreNodeVisitor.Builder();
        this.relationshipVisitorBuilder = new GraphStoreRelationshipVisitor.Builder();
        this.graphPropertyVisitorBuilder = new GraphStoreGraphPropertyVisitor.Builder();
        this.concurrency = concurrency;
        this.importPath = importPath;
        this.graphSchemaBuilder = ImmutableGraphSchema.builder();
        this.graphStoreBuilder = new GraphStoreBuilder()
            .concurrency(concurrency)
            .capabilities(ImmutableStaticCapabilities.of(true));
        this.log = log;
        this.taskRegistryFactory = taskRegistryFactory;
    }

    protected abstract FileInput fileInput(Path importPath);

    protected abstract String rootTaskName();

    public UserGraphStore run() {
        var fileInput = fileInput(importPath);
        this.progressTracker = createProgressTracker(fileInput);
        progressTracker.beginSubTask();
        try {
            importGraphStore(fileInput);
            graphStoreBuilder.schema(graphSchemaBuilder.build());
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

        if (!fileInput.graphPropertySchema().isEmpty()) {
            importTasks.add(Tasks.leaf("Import graph properties"));
        }

        var task = Tasks.task(
           rootTaskName() + " import",
            importTasks
        );

        return new TaskProgressTracker(task, log, concurrency, taskRegistryFactory);
    }

    private void importGraphStore(FileInput fileInput) {
        graphStoreBuilder.databaseId(fileInput.graphInfo().databaseId());
        graphStoreBuilder.capabilities(fileInput.capabilities());

        var nodes = importNodes(fileInput);
        importRelationships(fileInput, nodes);
        importGraphProperties(fileInput);
    }

    private IdMap importNodes(FileInput fileInput) {
        progressTracker.beginSubTask();
        NodeSchema nodeSchema = fileInput.nodeSchema();
        graphSchemaBuilder.nodeSchema(nodeSchema);

        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder(nodeSchema)
            .maxOriginalId(fileInput.graphInfo().maxOriginalId())
            .concurrency(concurrency)
            .nodeCount(fileInput.graphInfo().nodeCount())
            .deduplicateIds(false)
            .build();
        nodeVisitorBuilder.withNodeSchema(nodeSchema);
        nodeVisitorBuilder.withNodesBuilder(nodesBuilder);

        var nodesIterator = fileInput.nodes(Collector.EMPTY).iterator();
        Collection<Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            (index) -> new ElementImportRunner<>(nodeVisitorBuilder.build(), nodesIterator, progressTracker)
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var nodes = nodesBuilder.build();
        var nodeImportResultBuilder = ImmutableNodes.builder().idMap(nodes.idMap());

        var schemaProperties = nodeSchema.unionProperties();
        CSRGraphStoreUtil.extractNodeProperties(
            nodeImportResultBuilder,
            schemaProperties::get,
            nodes.properties().propertyValues()
        );

        graphStoreBuilder.nodes(nodeImportResultBuilder.build());

        progressTracker.endSubTask();
        return nodes.idMap();
    }

    private void importRelationships(FileInput fileInput, IdMap nodes) {
        progressTracker.beginSubTask();
        var relationshipBuildersByType = new ConcurrentHashMap<String, RelationshipsBuilder>();
        var relationshipSchema = fileInput.relationshipSchema();
        graphSchemaBuilder.relationshipSchema(relationshipSchema);

        this.relationshipVisitorBuilder
            .withRelationshipSchema(relationshipSchema)
            .withNodes(nodes)
            .withConcurrency(concurrency)
            .withAllocationTracker()
            .withRelationshipBuildersToTypeResultMap(relationshipBuildersByType)
            .withInverseIndexedRelationshipTypes(fileInput.graphInfo().inverseIndexedRelationshipTypes());

        var relationshipsIterator = fileInput.relationships(Collector.EMPTY).iterator();
        Collection<Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            (index) -> new ElementImportRunner<>(relationshipVisitorBuilder.build(), relationshipsIterator, progressTracker)
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var relationships = relationshipTopologyAndProperties(relationshipBuildersByType);
        var relationshipImportResult = RelationshipImportResult.of(
            relationships.topologies(),
            relationships.properties(),
            relationshipSchema.directions()
        );

        graphStoreBuilder.relationshipImportResult(relationshipImportResult);

        progressTracker.endSubTask();
    }

    private void importGraphProperties(FileInput fileInput) {
        if (!fileInput.graphPropertySchema().isEmpty()) {
            progressTracker.beginSubTask();

            var graphPropertySchema = fileInput.graphPropertySchema();
            graphSchemaBuilder.graphProperties(graphPropertySchema);
            graphPropertyVisitorBuilder.withGraphPropertySchema(graphPropertySchema);

            var graphStoreGraphPropertyVisitor = graphPropertyVisitorBuilder.build();

            var graphPropertiesIterator = fileInput.graphProperties().iterator();

            var tasks = ParallelUtil.tasks(
                concurrency,
                (index) -> new ElementImportRunner<>(
                    graphStoreGraphPropertyVisitor,
                    graphPropertiesIterator,
                    progressTracker
                )
            );
            ParallelUtil.run(tasks, Pools.DEFAULT);
            graphStoreGraphPropertyVisitor.close();

            graphStoreBuilder.graphProperties(GraphPropertyStoreFromVisitorHelper.fromGraphPropertyVisitor(
                graphPropertySchema,
                graphStoreGraphPropertyVisitor
            ));

            progressTracker.endSubTask();
        }
    }

    public static RelationshipTopologyAndProperties relationshipTopologyAndProperties(Map<String, RelationshipsBuilder> relationshipBuildersByType) {
        var propertyStores = new HashMap<RelationshipType, RelationshipPropertyStore>();
        var relationshipTypeTopologyMap = relationshipTypeToTopologyMapping(
            relationshipBuildersByType,
            propertyStores
        );

        var importedRelationships = relationshipTypeTopologyMap.values().stream().mapToLong(Topology::elementCount).sum();
        return ImmutableRelationshipTopologyAndProperties.of(relationshipTypeTopologyMap, propertyStores, importedRelationships);
    }

    private static Map<RelationshipType, Topology> relationshipTypeToTopologyMapping(
        Map<String, RelationshipsBuilder> relationshipBuildersByType,
        Map<RelationshipType, RelationshipPropertyStore> propertyStores
    ) {
        return relationshipBuildersByType.entrySet().stream().map(entry -> {
            var relationshipType = RelationshipType.of(entry.getKey());
            var relationships = entry.getValue().build();

            if (relationships.properties().isPresent()) {
                propertyStores.put(relationshipType, relationships.properties().get());
            }
            return Map.entry(relationshipType, relationships.topology());
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @ValueClass
    public interface UserGraphStore {
        String userName();
        GraphStore graphStore();
    }

    @ValueClass
    public interface RelationshipTopologyAndProperties {
        Map<RelationshipType, Topology> topologies();
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
