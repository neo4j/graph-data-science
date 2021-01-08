/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.export.GraphStoreExporter;
import org.neo4j.graphalgo.core.utils.export.GraphStoreInput;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvNodeVisitor;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipVisitor;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

public final class GraphStoreToFileExporter extends GraphStoreExporter<GraphStoreToFileExporterConfig> {

    private final VisitorProducer<NodeVisitor> nodeVisitorSupplier;
    private final VisitorProducer<RelationshipVisitor> relationshipVisitorSupplier;
    // assuming utf-8
    private static final int BYTES_PER_WRITTEN_CHARACTER = 8;
    private static final long DIGITS_PER_VALUE = (long) Math.ceil(Math.log(Long.MAX_VALUE));

    public static GraphStoreToFileExporter csv(
        GraphStore graphStore,
        GraphStoreToFileExporterConfig config,
        Path exportPath
    ) {
         Set<String> headerFiles = ConcurrentHashMap.newKeySet();

        return new GraphStoreToFileExporter(
            graphStore,
            config,
            (index) -> new CsvNodeVisitor(exportPath, graphStore.schema().nodeSchema(), headerFiles, index),
            (index) -> new CsvRelationshipVisitor(exportPath, graphStore.schema().relationshipSchema(), headerFiles, index)
        );
    }

    private GraphStoreToFileExporter(
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

    public static MemoryEstimation estimateCsvExport(GraphStore graphStore) {
        NodeSchema nodeSchema = graphStore.schema().nodeSchema();
        MemoryEstimations.Builder estimationsBuilder = MemoryEstimations
            .builder(GraphStoreToFileExporter.class);

        Stream<String> propertyKeys = graphStore.nodePropertyKeys().values().stream().flatMap(Collection::stream);
        Long nodePropertiesEstimate = propertyKeys.map(propertyKey -> {
            return graphStore.nodePropertyValues(propertyKey).size() * DIGITS_PER_VALUE * BYTES_PER_WRITTEN_CHARACTER;
        }).reduce(0L, Long::sum);

        // node id estimate
        long nodeIdsEstimate = getIdEstimate(graphStore);

        estimationsBuilder.fixed("Node data", nodeIdsEstimate + nodePropertiesEstimate);

        long avgBytesPerNodeId = nodeIdsEstimate / graphStore.nodeCount();
        // relationships
        Long relationshipEstimate = graphStore.relationshipTypes().stream().map(type -> {
            long relationshipCount = graphStore.getGraph(type).relationshipCount();
            long relationshipIdEstimate = relationshipCount * 2 * avgBytesPerNodeId;
            long relationshipValuesEstimate = relationshipCount * DIGITS_PER_VALUE * BYTES_PER_WRITTEN_CHARACTER;
            return relationshipIdEstimate + relationshipValuesEstimate;
        }).reduce(0L, Long::sum);

        estimationsBuilder.fixed("Relationship data", relationshipEstimate);

        return estimationsBuilder.build();
    }

    private static long getIdEstimate(GraphStore graphStore) {
        long maxNumberOfDigits = (long) Math.floor(Math.log(graphStore.nodeCount()));
        long nodeIdEstimate = 0;
        long consideredNumbers = 0;

        for (long digits = 1; digits < maxNumberOfDigits; digits++) {
            long numbersWithDigitX =(10 ^ digits) - consideredNumbers;
            consideredNumbers += numbersWithDigitX;

            nodeIdEstimate += numbersWithDigitX * digits * BYTES_PER_WRITTEN_CHARACTER;
        }

        nodeIdEstimate += (graphStore.nodeCount() - consideredNumbers) * maxNumberOfDigits * BYTES_PER_WRITTEN_CHARACTER;
        return nodeIdEstimate;
    }

    private void exportNodes(GraphStoreInput graphStoreInput) {
        var nodeInput = graphStoreInput.nodes(Collector.EMPTY);
        var nodeInputIterator = nodeInput.iterator();

        var tasks = ParallelUtil.tasks(
            config.writeConcurrency(),
            (index) -> new ImportRunner(nodeVisitorSupplier.apply(index), nodeInputIterator)
        );

        ParallelUtil.runWithConcurrency(config.writeConcurrency(), tasks, Pools.DEFAULT);
    }

    private void exportRelationships(GraphStoreInput graphStoreInput) {
        var relationshipInput = graphStoreInput.relationships(Collector.EMPTY);
        var relationshipInputIterator = relationshipInput.iterator();

        var tasks = ParallelUtil.tasks(
            config.writeConcurrency(),
            (index) -> new ImportRunner(relationshipVisitorSupplier.apply(index), relationshipInputIterator)
        );

        ParallelUtil.runWithConcurrency(config.writeConcurrency(), tasks, Pools.DEFAULT);
    }

    private static final class ImportRunner implements Runnable {
        private final ElementVisitor<?, ?, ?> visitor;
        private final InputIterator inputIterator;

        private ImportRunner(
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
}
