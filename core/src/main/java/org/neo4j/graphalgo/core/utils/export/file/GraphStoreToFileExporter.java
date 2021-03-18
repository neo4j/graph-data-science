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
package org.neo4j.graphalgo.core.utils.export.file;

import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.export.GraphStoreExporter;
import org.neo4j.graphalgo.core.utils.export.GraphStoreInput;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvGraphInfoVisitor;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvNodeSchemaVisitor;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvNodeVisitor;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipSchemaVisitor;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipVisitor;
import org.neo4j.graphalgo.core.utils.export.file.schema.NodeSchemaVisitor;
import org.neo4j.graphalgo.core.utils.export.file.schema.RelationshipSchemaVisitor;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class GraphStoreToFileExporter extends GraphStoreExporter<GraphStoreToFileExporterConfig> {

    protected final VisitorProducer<NodeVisitor> nodeVisitorSupplier;
    protected final VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier;

    public static GraphStoreToFileExporter csv(
        GraphStore graphStore,
        GraphStoreToFileExporterConfig config,
        Path exportPath
    ) {
        Set<String> headerFiles = ConcurrentHashMap.newKeySet();

        var nodeSchema = graphStore.schema().nodeSchema();
        var hasNodeProperties = !nodeSchema.allProperties().isEmpty();
        var relationshipSchema = graphStore.schema().relationshipSchema();
        var hasRelationshipProperties = !relationshipSchema.allProperties().isEmpty();
        return GraphStoreToFileExporter.of(
            graphStore,
            config,
            () -> new CsvGraphInfoVisitor(exportPath),
            () -> new CsvNodeSchemaVisitor(exportPath, hasNodeProperties),
            () -> new CsvRelationshipSchemaVisitor(exportPath, hasRelationshipProperties),
            (index) -> new CsvNodeVisitor(exportPath, nodeSchema, headerFiles, index, config.includeMetaData()),
            (index) -> new CsvRelationshipVisitor(exportPath,
                relationshipSchema, headerFiles, index)
        );
    }

    private static GraphStoreToFileExporter of(
        GraphStore graphStore,
        GraphStoreToFileExporterConfig config,
        Supplier<SingleRowVisitor<GraphInfo>> graphInfoVisitorSupplier,
        Supplier<NodeSchemaVisitor> nodeSchemaVisitorSupplier,
        Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitorSupplier,
        VisitorProducer<NodeVisitor> nodeVisitorSupplier,
        VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier
    ) {
        if (config.includeMetaData()) {
            return new FullGraphStoreToFileExporter(
                graphStore,
                config,
                graphInfoVisitorSupplier,
                nodeSchemaVisitorSupplier,
                relationshipSchemaVisitorSupplier,
                nodeVisitorSupplier,
                relationshipVisitorSupplier
            );
        } else {
            return new GraphStoreToFileExporter(graphStore, config, nodeVisitorSupplier, relationshipVisitorSupplier);
        }
    }

    protected GraphStoreToFileExporter(
        GraphStore graphStore,
        GraphStoreToFileExporterConfig config,
        VisitorProducer<NodeVisitor> nodeVisitorSupplier,
        VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier
    ) {
        super(graphStore, config);
        this.nodeVisitorSupplier = nodeVisitorSupplier;
        this.relationshipVisitorSupplier = relationshipVisitorSupplier;
    }

    @Override
    protected void export(GraphStoreInput graphStoreInput) {
        exportNodes(graphStoreInput);
        exportRelationships(graphStoreInput);
    }

    private void exportNodes(GraphStoreInput graphStoreInput) {
        var nodeInput = graphStoreInput.nodes(Collector.EMPTY);
        var nodeInputIterator = nodeInput.iterator();

        var tasks = ParallelUtil.tasks(
            config.writeConcurrency(),
            (index) -> new ElementImportRunner(nodeVisitorSupplier.apply(index), nodeInputIterator)
        );

        ParallelUtil.runWithConcurrency(config.writeConcurrency(), tasks, Pools.DEFAULT);
    }

    private void exportRelationships(GraphStoreInput graphStoreInput) {
        var relationshipInput = graphStoreInput.relationships(Collector.EMPTY);
        var relationshipInputIterator = relationshipInput.iterator();

        var tasks = ParallelUtil.tasks(
            config.writeConcurrency(),
            (index) -> new ElementImportRunner(relationshipVisitorSupplier.apply(index), relationshipInputIterator)
        );

        ParallelUtil.runWithConcurrency(config.writeConcurrency(), tasks, Pools.DEFAULT);
    }

    private static final class ElementImportRunner implements Runnable {
        private final ElementVisitor<?, ?, ?> visitor;
        private final InputIterator inputIterator;

        private ElementImportRunner(
            ElementVisitor<?, ?, ?> visitor,
            InputIterator inputIterator
        ) {
            this.visitor = visitor;
            this.inputIterator = inputIterator;
        }

        @Override
        public void run() {
            try (var chunk = inputIterator.newChunk()) {
                while (inputIterator.next(chunk)) {
                    while (chunk.next(visitor)) {

                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            visitor.close();
        }
    }

    private interface VisitorProducer<VISITOR> extends Function<Integer, VISITOR> {
    }

    private static final class FullGraphStoreToFileExporter extends GraphStoreToFileExporter {
        private final Supplier<SingleRowVisitor<GraphInfo>> graphInfoVisitorSupplier;
        private final Supplier<NodeSchemaVisitor> nodeSchemaVisitorSupplier;
        private final Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitorSupplier;

        private FullGraphStoreToFileExporter(
            GraphStore graphStore,
            GraphStoreToFileExporterConfig config,
            Supplier<SingleRowVisitor<GraphInfo>> graphInfoVisitorSupplier,
            Supplier<NodeSchemaVisitor> nodeSchemaVisitorSupplier,
            Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitorSupplier,
            VisitorProducer<NodeVisitor> nodeVisitorSupplier,
            VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier
        ) {
            super(graphStore, config, nodeVisitorSupplier, relationshipVisitorSupplier);
            this.graphInfoVisitorSupplier = graphInfoVisitorSupplier;
            this.nodeSchemaVisitorSupplier = nodeSchemaVisitorSupplier;
            this.relationshipSchemaVisitorSupplier = relationshipSchemaVisitorSupplier;
        }

        @Override
        protected void export(GraphStoreInput graphStoreInput) {
            exportGraphInfo(graphStoreInput);
            exportNodeSchema(graphStoreInput);
            exportRelationshipSchema(graphStoreInput);
            super.export(graphStoreInput);
        }

        private void exportGraphInfo(GraphStoreInput graphStoreInput) {
            GraphInfo graphInfo = graphStoreInput.metaDataStore().graphInfo();
            try (var namedDatabaseIdVisitor = graphInfoVisitorSupplier.get()) {
                namedDatabaseIdVisitor.export(graphInfo);
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
    }
}
