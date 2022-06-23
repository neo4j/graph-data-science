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
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.io.GraphStoreExporter;
import org.neo4j.gds.core.io.GraphStoreInput;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.core.io.schema.ElementSchemaVisitor;
import org.neo4j.gds.core.io.schema.NodeSchemaVisitor;
import org.neo4j.gds.core.io.schema.RelationshipSchemaVisitor;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.internal.batchimport.input.Collector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.function.Supplier;

public class GraphStoreToFileExporter extends GraphStoreExporter<GraphStoreToFileExporterConfig> {

    private final VisitorProducer<NodeVisitor> nodeVisitorSupplier;
    private final VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier;

    private final Supplier<SingleRowVisitor<String>> userInfoVisitorSupplier;
    private final Supplier<SingleRowVisitor<GraphInfo>> graphInfoVisitorSupplier;
    private final Supplier<NodeSchemaVisitor> nodeSchemaVisitorSupplier;
    private final Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitorSupplier;
    private final Supplier<SimpleWriter<Capabilities>> graphCapabilitiesWriterSupplier;

    public GraphStoreToFileExporter(
        GraphStore graphStore,
        GraphStoreToFileExporterConfig config,
        Optional<NeoNodeProperties> neoNodeProperties,
        Supplier<SingleRowVisitor<String>> userInfoVisitorSupplier,
        Supplier<SingleRowVisitor<GraphInfo>> graphInfoVisitorSupplier,
        Supplier<NodeSchemaVisitor> nodeSchemaVisitorSupplier,
        Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitorSupplier,
        Supplier<SimpleWriter<Capabilities>> graphCapabilitiesWriterSupplier,
        VisitorProducer<NodeVisitor> nodeVisitorSupplier,
        VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier
    ) {
        super(graphStore, config, neoNodeProperties);
        this.nodeVisitorSupplier = nodeVisitorSupplier;
        this.relationshipVisitorSupplier = relationshipVisitorSupplier;
        this.userInfoVisitorSupplier = userInfoVisitorSupplier;
        this.graphInfoVisitorSupplier = graphInfoVisitorSupplier;
        this.nodeSchemaVisitorSupplier = nodeSchemaVisitorSupplier;
        this.relationshipSchemaVisitorSupplier = relationshipSchemaVisitorSupplier;
        this.graphCapabilitiesWriterSupplier = graphCapabilitiesWriterSupplier;
    }

    @Override
    protected void export(GraphStoreInput graphStoreInput) {
        if (config.includeMetaData()) {
            exportUserName();
            exportGraphInfo(graphStoreInput);
            exportNodeSchema(graphStoreInput);
            exportRelationshipSchema(graphStoreInput);
            exportGraphCapabilities(graphStoreInput);
        }
        exportNodes(graphStoreInput);
        exportRelationships(graphStoreInput);
    }

    @Override
    protected GraphStoreExporter.IdMappingType idMappingType() {
        return IdMappingType.ORIGINAL;
    }

    private void exportNodes(GraphStoreInput graphStoreInput) {
        var nodeInput = graphStoreInput.nodes(Collector.EMPTY);
        var nodeInputIterator = nodeInput.iterator();

        var tasks = ParallelUtil.tasks(
            config.writeConcurrency(),
            (index) -> new ElementImportRunner(nodeVisitorSupplier.apply(index), nodeInputIterator, ProgressTracker.NULL_TRACKER)
        );

        RunWithConcurrency.builder()
            .concurrency(config.writeConcurrency())
            .tasks(tasks)
            .run();
    }

    private void exportRelationships(GraphStoreInput graphStoreInput) {
        var relationshipInput = graphStoreInput.relationships(Collector.EMPTY);
        var relationshipInputIterator = relationshipInput.iterator();

        var tasks = ParallelUtil.tasks(
            config.writeConcurrency(),
            (index) -> new ElementImportRunner(relationshipVisitorSupplier.apply(index), relationshipInputIterator, ProgressTracker.NULL_TRACKER)
        );

        RunWithConcurrency.builder()
            .concurrency(config.writeConcurrency())
            .tasks(tasks)
            .run();
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
            nodeSchema.properties().forEach((nodeLabel, properties) -> {
                if (properties.isEmpty()) {
                    nodeSchemaVisitor.nodeLabel(nodeLabel);
                    nodeSchemaVisitor.endOfEntity();
                } else {
                    properties.forEach((propertyKey, propertySchema) -> {
                        nodeSchemaVisitor.nodeLabel(nodeLabel);
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
            relationshipSchema.properties().forEach((relationshipType, properties) -> {
                if (properties.isEmpty()) {
                    relationshipSchemaVisitor.relationshipType(relationshipType);
                    relationshipSchemaVisitor.endOfEntity();
                } else {
                    properties.forEach((propertyKey, propertySchema) -> {
                        relationshipSchemaVisitor.relationshipType(relationshipType);
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

    private void exportGraphCapabilities(GraphStoreInput graphStoreInput) {
        var capabilitiesMapper = graphCapabilitiesWriterSupplier.get();
        try {
            capabilitiesMapper.write(graphStoreInput.capabilities());
        } catch (IOException e) {
            throw new UncheckedIOException(e);

        }
    }
}
