/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.write;

import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.utils.StatementApi;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.values.storable.Value;

import java.util.Arrays;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.write.NodePropertyExporter.MIN_BATCH_SIZE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class RelationshipStreamExporter extends StatementApi {

    private final LongUnaryOperator toOriginalId;
    private final Stream<Relationship> relationships;
    private final int batchSize;
    private final TerminationFlag terminationFlag;
    private final ProgressLogger progressLogger;

    @ValueClass
    public interface Relationship {
        long sourceNode();

        long targetNode();

        @org.immutables.value.Value.Default
        default Value @Nullable [] values() {
            return null;
        }
    }

    public static RelationshipStreamExporter.Builder builder(
        GraphDatabaseService db,
        IdMapping idMapping,
        Stream<Relationship> relationships,
        TerminationFlag terminationFlag
    ) {
        return new RelationshipStreamExporter.Builder(
            SecureTransaction.of(db),
            idMapping,
            relationships,
            terminationFlag
        );
    }

    public static final class Builder extends ExporterBuilder<RelationshipStreamExporter> {

        private final Stream<Relationship> relationships;
        private int batchSize;

        Builder(
            SecureTransaction tx,
            IdMapping idMapping,
            Stream<Relationship> relationships,
            TerminationFlag terminationFlag
        ) {
            super(tx, idMapping, terminationFlag);
            this.relationships = relationships;
            this.batchSize = (int) MIN_BATCH_SIZE;
        }

        @Override
        public RelationshipStreamExporter build() {
            return new RelationshipStreamExporter(
                tx,
                toOriginalId,
                relationships,
                batchSize,
                terminationFlag,
                progressLogger
            );
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        @Override
        String taskName() {
            return "WriteRelationshipStream";
        }

        @Override
        long taskVolume() {
            // We write relationships from a finite stream.
            // The number of relationships is therefore not
            // known upfront.
            return 0;
        }
    }

    private RelationshipStreamExporter(
        SecureTransaction tx,
        LongUnaryOperator toOriginalId,
        Stream<Relationship> relationships,
        int batchSize,
        TerminationFlag terminationFlag,
        ProgressLogger progressLogger
    ) {
        super(tx);
        this.toOriginalId = toOriginalId;
        this.relationships = relationships;
        this.batchSize = batchSize;
        this.terminationFlag = terminationFlag;
        this.progressLogger = progressLogger;
    }

    public long write(String relationshipType, String... propertyKeys) {
        var relationshipToken = getOrCreateRelationshipToken(relationshipType);
        var propertyTokens = Arrays.stream(propertyKeys).mapToInt(this::getOrCreatePropertyToken).toArray();

        progressLogger.logStart();

        var written = new MutableLong(0);

        var buffer = new Buffer(batchSize);

        relationships.forEach(relationship -> {
            buffer.add(relationship);
            if (buffer.isFull()) {
                written.add(write(buffer, relationshipToken, propertyTokens));
                buffer.reset();
                progressLogger.logMessage(formatWithLocale("Wrote %d relationships", written.longValue()));
            }
        });

        // write final relationships
        written.add(write(buffer, relationshipToken, propertyTokens));
        progressLogger.logMessage(formatWithLocale("Wrote %d relationships", written.longValue()));
        progressLogger.logFinish();

        return written.longValue();
    }

    private int write(Buffer buffer, int relationshipToken, int[] propertyTokens) {
        var bufferSize = buffer.size;
        var tokenCount = propertyTokens.length;
        var relationships = buffer.array;

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

    static class Buffer {
        private final long capacity;
        private final Relationship[] array;
        private int size;

        Buffer(int capacity) {
            this.array = new Relationship[(int) capacity];
            this.capacity = capacity;
        }

        void add(Relationship relationship) {
            array[size] = relationship;
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
