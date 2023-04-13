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
package org.neo4j.gds.core.write;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.StatementApi;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

public final class NativeRelationshipStreamExporter extends StatementApi implements RelationshipStreamExporter {

    private static final int QUEUE_CAPACITY = 2;

    private final LongUnaryOperator toOriginalId;
    private final Stream<ExportedRelationship> relationships;
    private final int batchSize;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;

    public static RelationshipStreamExporterBuilder builder(
        TransactionContext transactionContext,
        IdMap idMap,
        Stream<ExportedRelationship> relationships,
        TerminationFlag terminationFlag
    ) {
        return new NativeRelationshipStreamExporterBuilder(transactionContext)
            .withRelationships(relationships)
            .withIdMappingOperator(idMap::toOriginalNodeId)
            .withTerminationFlag(terminationFlag);
    }

    NativeRelationshipStreamExporter(
        TransactionContext tx,
        LongUnaryOperator toOriginalId,
        Stream<ExportedRelationship> relationships,
        int batchSize,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        super(tx);
        this.toOriginalId = toOriginalId;
        this.relationships = relationships.sequential();
        this.batchSize = batchSize;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
    }

    @Override
    public long write(String relationshipType, List<String> propertyKeys, List<ValueType> __) {
        progressTracker.beginSubTask();

        try {
            var relationshipToken = getOrCreateRelationshipToken(relationshipType);
            var propertyTokens = propertyKeys.stream().mapToInt(this::getOrCreatePropertyToken).toArray();

            var writeQueue = new LinkedBlockingQueue<Buffer>(QUEUE_CAPACITY);
            var bufferPool = new LinkedBlockingQueue<Buffer>(QUEUE_CAPACITY);
            for (int i = 0; i < QUEUE_CAPACITY; i++) {
                bufferPool.add(new Buffer(batchSize));
            }

            var writer = new Writer(
                tx,
                progressTracker,
                toOriginalId,
                writeQueue,
                bufferPool,
                relationshipToken,
                propertyTokens,
                terminationFlag
            );
            var consumer = Pools.DEFAULT.submit(writer);

            var bufferRef = new AtomicReference<>(bufferPool.poll());

            relationships.forEach(relationship -> {
                var buffer = bufferRef.get();
                buffer.add(relationship);
                if (buffer.isFull()) {
                    try {
                        writeQueue.put(buffer);
                        bufferRef.set(bufferPool.take());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            });

            try {
                writeQueue.put(bufferRef.get());
                // Add an empty buffer to signal end of writing
                writeQueue.put(new Buffer(0));
                consumer.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            return writer.written;
        } finally {
            progressTracker.endSubTask();
        }
    }

    static class Writer extends StatementApi implements Runnable {

        private final TerminationFlag terminationFlag;
        private final ProgressTracker progressTracker;

        private final LongUnaryOperator toOriginalId;
        private final BlockingQueue<Buffer> writeQueue;
        private final BlockingQueue<Buffer> bufferPool;

        private final int relationshipToken;
        private final int[] propertyTokens;
        private long written;

        Writer(
            TransactionContext tx,
            ProgressTracker progressTracker,
            LongUnaryOperator toOriginalId,
            BlockingQueue<Buffer> writeQueue,
            BlockingQueue<Buffer> bufferPool,
            int relationshipToken,
            int[] propertyTokens,
            TerminationFlag terminationFlag
        ) {
            super(tx);
            this.progressTracker = progressTracker;
            this.toOriginalId = toOriginalId;
            this.writeQueue = writeQueue;
            this.bufferPool = bufferPool;
            this.relationshipToken = relationshipToken;
            this.propertyTokens = propertyTokens;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public void run() {
            Buffer buffer;
            while (true) {
                try {
                    buffer = writeQueue.take();
                    if (buffer.size == 0) {
                        return;
                    }
                    written += write(buffer, relationshipToken, propertyTokens);

                    progressTracker.logProgress(written, "has written %d relationships");

                    buffer.reset();
                    bufferPool.put(buffer);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }

        private int write(Buffer buffer, int relationshipToken, int[] propertyTokens) {
            var bufferSize = buffer.size;
            var tokenCount = propertyTokens.length;
            var relationships = buffer.relationships;

            acceptInTransaction(stmt -> {
                terminationFlag.assertRunning();
                var ops = stmt.dataWrite();

                for (int i = 0; i < bufferSize; i++) {
                    // create relationship
                    long relationshipId = ops.relationshipCreate(
                        toOriginalId.applyAsLong(relationships[i].sourceNode()),
                        relationshipToken,
                        toOriginalId.applyAsLong(relationships[i].targetNode())
                    );

                    // write properties
                    var values = relationships[i].values();
                    for (int j = 0; j < tokenCount; j++) {
                        ops.relationshipSetProperty(relationshipId, propertyTokens[j], values[j]);
                    }
                }
            });

            return bufferSize;
        }
    }

    static class Buffer {
        private final long capacity;
        private final ExportedRelationship[] relationships;
        private int size;

        Buffer(int capacity) {
            this.relationships = new ExportedRelationship[capacity];
            this.capacity = capacity;
        }

        void add(ExportedRelationship relationship) {
            relationships[size] = relationship;
            size += 1;
        }

        boolean isFull() {
            return size == capacity;
        }

        void reset() {
            this.size = 0;
        }
    }
}
