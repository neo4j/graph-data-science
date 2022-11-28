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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.CompatInput;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.io.GraphStoreExporter;
import org.neo4j.gds.core.io.GraphStoreInput;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.core.io.schema.ElementSchemaVisitor;
import org.neo4j.gds.core.io.schema.NodeSchemaVisitor;
import org.neo4j.gds.core.io.schema.RelationshipSchemaVisitor;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Supplier;

public class GraphStoreToFileExporter extends GraphStoreExporter<GraphStoreToFileExporterConfig> {

    private final VisitorProducer<NodeVisitor> nodeVisitorSupplier;
    private final VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier;
    private final VisitorProducer<GraphPropertyVisitor> graphPropertyVisitorSupplier;

    private final Supplier<SingleRowVisitor<String>> userInfoVisitorSupplier;
    private final Supplier<SingleRowVisitor<GraphInfo>> graphInfoVisitorSupplier;
    private final Supplier<NodeSchemaVisitor> nodeSchemaVisitorSupplier;
    private final Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitorSupplier;
    private final Supplier<ElementSchemaVisitor> graphPropertySchemaVisitorSupplier;
    private final Supplier<SimpleWriter<Capabilities>> graphCapabilitiesWriterSupplier;

    private final TaskRegistryFactory taskRegistryFactory;
    private final Log log;
    private final String rootTaskName;

    public GraphStoreToFileExporter(
        GraphStore graphStore,
        GraphStoreToFileExporterConfig config,
        Optional<NeoNodeProperties> neoNodeProperties,
        Supplier<SingleRowVisitor<String>> userInfoVisitorSupplier,
        Supplier<SingleRowVisitor<GraphInfo>> graphInfoVisitorSupplier,
        Supplier<NodeSchemaVisitor> nodeSchemaVisitorSupplier,
        Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitorSupplier,
        Supplier<ElementSchemaVisitor> graphPropertySchemaVisitorSupplier,
        Supplier<SimpleWriter<Capabilities>> graphCapabilitiesWriterSupplier,
        VisitorProducer<NodeVisitor> nodeVisitorSupplier,
        VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier,
        VisitorProducer<GraphPropertyVisitor> graphPropertyVisitorSupplier,
        TaskRegistryFactory taskRegistryFactory,
        Log log,
        String rootTaskName
    ) {
        super(graphStore, config, neoNodeProperties);
        this.nodeVisitorSupplier = nodeVisitorSupplier;
        this.relationshipVisitorSupplier = relationshipVisitorSupplier;
        this.graphPropertyVisitorSupplier = graphPropertyVisitorSupplier;
        this.userInfoVisitorSupplier = userInfoVisitorSupplier;
        this.graphInfoVisitorSupplier = graphInfoVisitorSupplier;
        this.nodeSchemaVisitorSupplier = nodeSchemaVisitorSupplier;
        this.relationshipSchemaVisitorSupplier = relationshipSchemaVisitorSupplier;
        this.graphPropertySchemaVisitorSupplier = graphPropertySchemaVisitorSupplier;
        this.graphCapabilitiesWriterSupplier = graphCapabilitiesWriterSupplier;
        this.taskRegistryFactory = taskRegistryFactory;
        this.log = log;
        this.rootTaskName = rootTaskName;
    }

    @Override
    protected void export(GraphStoreInput graphStoreInput) {
        if (config.includeMetaData()) {
            exportUserName();
            exportGraphInfo(graphStoreInput);
            exportNodeSchema(graphStoreInput);
            exportRelationshipSchema(graphStoreInput);
            exportGraphPropertySchema(graphStoreInput);
            exportGraphCapabilities(graphStoreInput);
        }
        var progressTracker = createProgressTracker(graphStoreInput);
        progressTracker.beginSubTask();
        exportNodes(graphStoreInput, progressTracker);
        exportRelationships(graphStoreInput, progressTracker);
        exportGraphProperties(graphStoreInput, progressTracker);
        progressTracker.endSubTask();
    }

    @Override
    protected GraphStoreExporter.IdMappingType idMappingType() {
        return IdMappingType.ORIGINAL;
    }

    private ProgressTracker createProgressTracker(GraphStoreInput graphStoreInput) {
        var graphInfo = graphStoreInput.metaDataStore().graphInfo();

        var importTasks = new ArrayList<Task>();
        importTasks.add(Tasks.leaf("Export nodes", graphInfo.nodeCount()));
        importTasks.add(Tasks.leaf(
            "Export relationships",
            graphInfo.relationshipTypeCounts().values().stream().mapToLong(Long::longValue).sum()
        ));

        if (!graphStoreInput.metaDataStore().graphPropertySchema().isEmpty()) {
            importTasks.add(Tasks.leaf("Export graph properties"));
        }

        var task = Tasks.task(rootTaskName + " export", importTasks);
        return new TaskProgressTracker(task, log, config.writeConcurrency(), taskRegistryFactory);
    }

    private void exportNodes(
        CompatInput graphStoreInput,
        ProgressTracker progressTracker
    ) {
        progressTracker.beginSubTask();
        var nodeInput = graphStoreInput.nodes(Collector.EMPTY);
        var nodeInputIterator = nodeInput.iterator();

        var tasks = ParallelUtil.tasks(
            config.writeConcurrency(),
            (index) -> new ElementImportRunner<>(nodeVisitorSupplier.apply(index), nodeInputIterator, progressTracker)
        );

        RunWithConcurrency.builder()
            .concurrency(config.writeConcurrency())
            .tasks(tasks)
            .run();
        progressTracker.endSubTask();
    }

    private void exportRelationships(
        CompatInput graphStoreInput,
        ProgressTracker progressTracker
    ) {
        progressTracker.beginSubTask();
        var relationshipInput = graphStoreInput.relationships(Collector.EMPTY);
        var relationshipInputIterator = relationshipInput.iterator();

        var tasks = ParallelUtil.tasks(
            config.writeConcurrency(),
            (index) -> new ElementImportRunner<>(
                relationshipVisitorSupplier.apply(index),
                relationshipInputIterator,
                progressTracker
            )
        );

        RunWithConcurrency.builder()
            .concurrency(config.writeConcurrency())
            .tasks(tasks)
            .mayInterruptIfRunning(false)
            .run();
        progressTracker.endSubTask();
    }

    private void exportGraphProperties(
        GraphStoreInput graphStoreInput,
        ProgressTracker progressTracker
    ) {
        if (!graphStoreInput.metaDataStore().graphPropertySchema().isEmpty()) {
            progressTracker.beginSubTask();
            var graphPropertyInput = graphStoreInput.graphProperties();
            var graphPropertyInputIterator = graphPropertyInput.iterator();

            var tasks = ParallelUtil.tasks(
                config.writeConcurrency(),
                (index) -> new ElementImportRunner<>(
                    graphPropertyVisitorSupplier.apply(index),
                    graphPropertyInputIterator,
                    progressTracker
                )
            );

            RunWithConcurrency.builder()
                .concurrency(config.writeConcurrency())
                .tasks(tasks)
                .run();
            progressTracker.endSubTask();
        }
    }

    private void exportUserName() {
        try (var userInfoVisitor = userInfoVisitorSupplier.get()) {
            userInfoVisitor.export(config.username());
        }
    }

    private void exportGraphInfo(GraphStoreInput graphStoreInput) {
        GraphInfo graphInfo = graphStoreInput.metaDataStore().graphInfo();
        try (var graphInfoVisitor = graphInfoVisitorSupplier.get()) {
            graphInfoVisitor.export(graphInfo);
        }
    }

    private void exportNodeSchema(GraphStoreInput graphStoreInput) {
        var nodeSchema = graphStoreInput.metaDataStore().nodeSchema();
        try (var nodeSchemaVisitor = nodeSchemaVisitorSupplier.get()) {
            nodeSchema.entries().forEach(nodeEntry -> {
                if (nodeEntry.properties().isEmpty()) {
                    nodeSchemaVisitor.nodeLabel(nodeEntry.identifier());
                    nodeSchemaVisitor.endOfEntity();
                } else {
                    nodeEntry.properties().forEach((propertyKey, propertySchema) -> {
                        nodeSchemaVisitor.nodeLabel(nodeEntry.identifier());
                        nodeSchemaVisitor.key(propertyKey);
                        nodeSchemaVisitor.defaultValue(propertySchema.defaultValue());
                        nodeSchemaVisitor.valueType(propertySchema.valueType());
                        nodeSchemaVisitor.state(propertySchema.state());
                        nodeSchemaVisitor.endOfEntity();
                    });
                }
            });
        }
    }

    private void exportRelationshipSchema(GraphStoreInput graphStoreInput) {
        var relationshipSchema = graphStoreInput.metaDataStore().relationshipSchema();
        try (var relationshipSchemaVisitor = relationshipSchemaVisitorSupplier.get()) {
            relationshipSchema.entries().forEach(relationshipEntry -> {
                if (relationshipEntry.properties().isEmpty()) {
                    relationshipSchemaVisitor.relationshipType(relationshipEntry.identifier());
                    relationshipSchemaVisitor.direction(relationshipEntry.direction());
                    relationshipSchemaVisitor.endOfEntity();
                } else {
                    relationshipEntry.properties().forEach((propertyKey, propertySchema) -> {
                        relationshipSchemaVisitor.relationshipType(relationshipEntry.identifier());
                        relationshipSchemaVisitor.direction(relationshipEntry.direction());
                        relationshipSchemaVisitor.key(propertyKey);
                        relationshipSchemaVisitor.defaultValue(propertySchema.defaultValue());
                        relationshipSchemaVisitor.valueType(propertySchema.valueType());
                        relationshipSchemaVisitor.aggregation(propertySchema.aggregation());
                        relationshipSchemaVisitor.state(propertySchema.state());
                        relationshipSchemaVisitor.endOfEntity();
                    });
                }
            });
        }
    }

    private void exportGraphPropertySchema(GraphStoreInput graphStoreInput) {
        var graphPropertySchema = graphStoreInput.metaDataStore().graphPropertySchema();
        try (var graphPropertySchemaVisitor = graphPropertySchemaVisitorSupplier.get()) {
            graphPropertySchema.forEach((key, propertySchema) -> {
                graphPropertySchemaVisitor.key(key);
                graphPropertySchemaVisitor.defaultValue(propertySchema.defaultValue());
                graphPropertySchemaVisitor.valueType(propertySchema.valueType());
                graphPropertySchemaVisitor.state(propertySchema.state());
                graphPropertySchemaVisitor.endOfEntity();
            });
        }
    }

    private void exportGraphCapabilities(GraphStoreInput graphStoreInput) {
        var capabilitiesMapper = graphCapabilitiesWriterSupplier.get();
        try {
            capabilitiesMapper.write(graphStoreInput.capabilities());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
