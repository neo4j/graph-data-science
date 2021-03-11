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
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvNamedDatabaseIdVisitor;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvNodeSchemaVisitor;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvNodeVisitor;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipSchemaVisitor;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipVisitor;
import org.neo4j.graphalgo.core.utils.export.file.schema.NodeSchemaVisitor;
import org.neo4j.graphalgo.core.utils.export.file.schema.RelationshipSchemaVisitor;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class GraphStoreToFileExporter extends GraphStoreExporter<GraphStoreToFileExporterConfig> {

    protected final VisitorProducer<NodeVisitor> nodeVisitorSupplier;
    protected final VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier;

    public enum Mode {
        FULL,
        PARTIAL
    }

    public static GraphStoreToFileExporter csv(
        GraphStore graphStore,
        GraphStoreToFileExporterConfig config,
        Path exportPath
    ) {
         Set<String> headerFiles = ConcurrentHashMap.newKeySet();

        return GraphStoreToFileExporter.of(
            graphStore,
            config,
            () -> new CsvNamedDatabaseIdVisitor(exportPath),
            () -> new CsvNodeSchemaVisitor(exportPath),
            () -> new CsvRelationshipSchemaVisitor(exportPath),
            (index) -> new CsvNodeVisitor(exportPath, graphStore.schema().nodeSchema(), headerFiles, index, config.exportNeoNodeIds()),
            (index) -> new CsvRelationshipVisitor(exportPath, graphStore.schema().relationshipSchema(), headerFiles, index)
        );
    }

    private static GraphStoreToFileExporter of(
        GraphStore graphStore,
        GraphStoreToFileExporterConfig config,
        Supplier<SingleRowVisitor<NamedDatabaseId>> namedDatabaseIdVisitor,
        Supplier<NodeSchemaVisitor> nodeSchemaVisitor,
        Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitor,
        VisitorProducer<NodeVisitor> nodeVisitorSupplier,
        VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier
    ) {
        Mode mode = config.mode();
        if (mode == Mode.FULL) {
            return new FullGraphStoreToFileExporter(
                graphStore,
                config,
                namedDatabaseIdVisitor,
                nodeSchemaVisitor,
                relationshipSchemaVisitor,
                nodeVisitorSupplier,
                relationshipVisitorSupplier
            );
        }
        if (mode == Mode.PARTIAL) {
            return new GraphStoreToFileExporter(graphStore, config, nodeVisitorSupplier, relationshipVisitorSupplier);
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected config parameter `mode` to be one of %s, but was %s",
            Arrays.toString(Mode.values()),
            mode
        ));
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
        private final Supplier<SingleRowVisitor<NamedDatabaseId>> namedDatabaseIdVisitorSupplier;
        private final Supplier<NodeSchemaVisitor> nodeSchemaVisitorSupplier;
        private final Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitorSupplier;

        private FullGraphStoreToFileExporter(
            GraphStore graphStore,
            GraphStoreToFileExporterConfig config,
            Supplier<SingleRowVisitor<NamedDatabaseId>> namedDatabaseIdVisitorSupplier,
            Supplier<NodeSchemaVisitor> nodeSchemaVisitorSupplier,
            Supplier<RelationshipSchemaVisitor> relationshipSchemaVisitorSupplier,
            VisitorProducer<NodeVisitor> nodeVisitorSupplier,
            VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier
        ) {
            super(graphStore, config, nodeVisitorSupplier, relationshipVisitorSupplier);
            this.namedDatabaseIdVisitorSupplier = namedDatabaseIdVisitorSupplier;
            this.nodeSchemaVisitorSupplier = nodeSchemaVisitorSupplier;
            this.relationshipSchemaVisitorSupplier = relationshipSchemaVisitorSupplier;
        }

        @Override
        protected void export(GraphStoreInput graphStoreInput) {
            exportNamedDatabaseId(graphStoreInput);
            exportNodeSchema(graphStoreInput);
            exportRelationshipSchema(graphStoreInput);
            super.export(graphStoreInput);
        }

        private void exportNamedDatabaseId(GraphStoreInput graphStoreInput) {
            NamedDatabaseId namedDatabaseId = graphStoreInput.metaDataStore().databaseId();
            try (var namedDatabaseIdVisitor = namedDatabaseIdVisitorSupplier.get()) {
                namedDatabaseIdVisitor.export(namedDatabaseId);
            }
        }

        private void exportNodeSchema(GraphStoreInput graphStoreInput) {
            var nodeSchema = graphStoreInput.metaDataStore().nodeSchema();
            try (var nodeSchemaVisitor = nodeSchemaVisitorSupplier.get()) {
                nodeSchema.properties().forEach((nodeLabel, properties) -> {
                    properties.forEach((propertyKey, propertySchema) -> {
                        nodeSchemaVisitor.nodeLabel(nodeLabel);
                        nodeSchemaVisitor.key(propertyKey);
                        nodeSchemaVisitor.defaultValue(propertySchema.defaultValue());
                        nodeSchemaVisitor.valueType(propertySchema.valueType());
                        nodeSchemaVisitor.state(propertySchema.state());
                        nodeSchemaVisitor.endOfEntity();
                    });
                });
            }
        }

        private void exportRelationshipSchema(GraphStoreInput graphStoreInput) {
            var relationshipSchema = graphStoreInput.metaDataStore().relationshipSchema();
            try (var relationshipSchemaVisitor = relationshipSchemaVisitorSupplier.get()) {
                relationshipSchema.properties().forEach((relationshipType, properties) -> {
                    properties.forEach((propertyKey, propertySchema) -> {
                        relationshipSchemaVisitor.relationshipType(relationshipType);
                        relationshipSchemaVisitor.key(propertyKey);
                        relationshipSchemaVisitor.defaultValue(propertySchema.defaultValue());
                        relationshipSchemaVisitor.valueType(propertySchema.valueType());
                        relationshipSchemaVisitor.aggregation(propertySchema.aggregation());
                        relationshipSchemaVisitor.state(propertySchema.state());
                        relationshipSchemaVisitor.endOfEntity();
                    });
                });
            }
        }
    }
}
