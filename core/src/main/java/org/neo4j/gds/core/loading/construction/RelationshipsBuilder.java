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
package org.neo4j.gds.core.loading.construction;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.compress.AdjacencyCompressor;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.PropertyReader;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporter;
import org.neo4j.gds.core.loading.ThreadLocalSingleTypeRelationshipImporter;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RelationshipsBuilder {

    private final PartialIdMap idMap;

    private final boolean loadRelationshipProperty;
    private final boolean isMultiGraph;

    private final Orientation orientation;
    private final SingleTypeRelationshipImporter singleTypeRelationshipImporter;

    private final int concurrency;
    private final ExecutorService executorService;

    private final AutoCloseableThreadLocal<ThreadLocalBuilder> threadLocalBuilders;

    RelationshipsBuilder(
        PartialIdMap idMap,
        Orientation orientation,
        int bufferSize,
        int[] propertyKeyIds,
        SingleTypeRelationshipImporter singleTypeRelationshipImporter,
        boolean loadRelationshipProperty,
        boolean isMultiGraph,
        int concurrency,
        ExecutorService executorService
    ) {
        this.idMap = idMap;
        this.orientation = orientation;
        this.singleTypeRelationshipImporter = singleTypeRelationshipImporter;
        this.loadRelationshipProperty = loadRelationshipProperty;
        this.isMultiGraph = isMultiGraph;
        this.concurrency = concurrency;
        this.executorService = executorService;

        this.threadLocalBuilders = AutoCloseableThreadLocal.withInitial(() -> new ThreadLocalBuilder(
            idMap,
            singleTypeRelationshipImporter,
            bufferSize,
            propertyKeyIds
        ));
    }

    public void add(long source, long target) {
        addFromInternal(idMap.toMappedNodeId(source), idMap.toMappedNodeId(target));
    }

    public void add(long source, long target, double relationshipPropertyValue) {
        addFromInternal(
            idMap.toMappedNodeId(source),
            idMap.toMappedNodeId(target),
            relationshipPropertyValue
        );
    }

    public void add(long source, long target, double[] relationshipPropertyValues) {
        addFromInternal(
            idMap.toMappedNodeId(source),
            idMap.toMappedNodeId(target),
            relationshipPropertyValues
        );
    }

    public <T extends Relationship> void add(Stream<T> relationshipStream) {
        relationshipStream.forEach(this::add);
    }

    public <T extends Relationship> void add(T relationship) {
        add(relationship.sourceNodeId(), relationship.targetNodeId(), relationship.property());
    }

    public <T extends Relationship> void addFromInternal(Stream<T> relationshipStream) {
        relationshipStream.forEach(this::addFromInternal);
    }

    public <T extends Relationship> void addFromInternal(T relationship) {
        addFromInternal(relationship.sourceNodeId(), relationship.targetNodeId(), relationship.property());
    }

    public void addFromInternal(long source, long target) {
        threadLocalBuilders.get().addRelationship(source, target);
    }

    public void addFromInternal(long source, long target, double relationshipPropertyValue) {
        threadLocalBuilders.get().addRelationship(
            source,
            target,
            relationshipPropertyValue
        );
    }

    public void addFromInternal(long source, long target, double[] relationshipPropertyValues) {
        threadLocalBuilders.get().addRelationship(
            source,
            target,
            relationshipPropertyValues
        );
    }

    public RelationshipsAndOrientation build() {
        return buildAll().get(0);
    }

    public List<RelationshipsAndOrientation> buildAll() {
        return buildAll(Optional.empty(), Optional.empty());
    }

    /**
     * @param mapper             A mapper to transform values before compressing them. Implementations must be thread-safe.
     * @param drainCountConsumer A consumer which is called once a {@link org.neo4j.gds.core.loading.ChunkedAdjacencyLists}
     *                           has been drained and its contents are written to the adjacency list. The consumer receives the number
     *                           of relationships that have been written. Implementations must be thread-safe.
     */
    public List<RelationshipsAndOrientation> buildAll(
        Optional<AdjacencyCompressor.ValueMapper> mapper,
        Optional<LongConsumer> drainCountConsumer
    ) {
        threadLocalBuilders.close();

        var adjacencyListBuilderTasks = singleTypeRelationshipImporter.adjacencyListBuilderTasks(
            mapper,
            drainCountConsumer
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(adjacencyListBuilderTasks)
            .executor(executorService)
            .run();

        var adjacencyListsWithProperties = singleTypeRelationshipImporter.build();
        var adjacencyList = adjacencyListsWithProperties.adjacency();
        var relationshipCount = adjacencyListsWithProperties.relationshipCount();

        if (loadRelationshipProperty) {
            return adjacencyListsWithProperties.properties().stream().map(compressedProperties ->
                {
                    var relationships = Relationships.of(
                        relationshipCount,
                        isMultiGraph,
                        adjacencyList,
                        compressedProperties,
                        DefaultValue.DOUBLE_DEFAULT_FALLBACK
                    );
                    return RelationshipsAndOrientation.of(relationships, orientation);
                }
            ).collect(Collectors.toList());
        } else {
            var relationships = Relationships.of(
                relationshipCount,
                isMultiGraph,
                adjacencyList
            );

            return List.of(RelationshipsAndOrientation.of(relationships, orientation));
        }
    }

    private static class ThreadLocalBuilder implements AutoCloseable {

        private final ThreadLocalSingleTypeRelationshipImporter importer;
        private final PropertyReader.Buffered bufferedPropertyReader;
        private final int[] propertyKeyIds;

        private int localRelationshipId;

        ThreadLocalBuilder(
            PartialIdMap idMap,
            SingleTypeRelationshipImporter singleTypeRelationshipImporter,
            int bufferSize,
            int[] propertyKeyIds
        ) {
            this.propertyKeyIds = propertyKeyIds;

            if (propertyKeyIds.length > 1) {
                this.bufferedPropertyReader = PropertyReader.buffered(bufferSize, propertyKeyIds.length);
                this.importer = singleTypeRelationshipImporter.threadLocalImporter(
                    idMap,
                    bufferSize,
                    bufferedPropertyReader
                );
            } else {
                this.bufferedPropertyReader = null;
                this.importer = singleTypeRelationshipImporter.threadLocalImporter(
                    idMap,
                    bufferSize,
                    PropertyReader.preLoaded()
                );
            }
        }

        void addRelationship(long source, long target) {
            importer.buffer().add(source, target);
            if (importer.buffer().isFull()) {
                flushBuffer();
            }
        }

        void addRelationship(long source, long target, double relationshipPropertyValue) {
            importer
                .buffer()
                .add(
                    source,
                    target,
                    Double.doubleToLongBits(relationshipPropertyValue),
                    Neo4jProxy.noPropertyReference()
                );
            if (importer.buffer().isFull()) {
                flushBuffer();
            }
        }

        void addRelationship(long source, long target, double[] relationshipPropertyValues) {
            int nextRelationshipId = localRelationshipId++;
            importer.buffer().add(source, target, nextRelationshipId, Neo4jProxy.noPropertyReference());
            int[] keyIds = propertyKeyIds;
            for (int i = 0; i < keyIds.length; i++) {
                bufferedPropertyReader.add(nextRelationshipId, keyIds[i], relationshipPropertyValues[i]);
            }
            if (importer.buffer().isFull()) {
                flushBuffer();
            }
        }

        private void flushBuffer() {
            importer.importRelationships();
            importer.buffer().reset();
            localRelationshipId = 0;
        }

        @Override
        public void close() {
            flushBuffer();
        }
    }

    public interface Relationship {
        long sourceNodeId();

        long targetNodeId();

        double property();
    }
}
