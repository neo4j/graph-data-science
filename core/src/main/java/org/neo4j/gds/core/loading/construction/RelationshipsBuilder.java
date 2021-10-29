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
import org.neo4j.gds.api.IdMapping;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.AdjacencyListWithPropertiesBuilder;
import org.neo4j.gds.core.loading.RelationshipImporter;
import org.neo4j.gds.core.loading.RelationshipPropertiesBatchBuffer;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporter;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RelationshipsBuilder {

    private final IdMapping idMapping;

    private final boolean loadRelationshipProperty;
    private final boolean isMultiGraph;

    private final AdjacencyListWithPropertiesBuilder adjacencyListWithPropertiesBuilder;
    private final Orientation orientation;
    private final SingleTypeRelationshipImporter.Builder.WithImporter importerBuilder;
    private final LongAdder relationshipCounter;

    private final int concurrency;
    private final ExecutorService executorService;

    private final AutoCloseableThreadLocal<ThreadLocalBuilder> threadLocalBuilders;

    RelationshipsBuilder(
        IdMapping idMapping,
        Orientation orientation,
        int bufferSize,
        int[] propertyKeyIds,
        AdjacencyListWithPropertiesBuilder adjacencyListWithPropertiesBuilder,
        SingleTypeRelationshipImporter.Builder.WithImporter importerBuilder,
        LongAdder relationshipCounter,
        boolean loadRelationshipProperty,
        boolean isMultiGraph,
        int concurrency,
        ExecutorService executorService
    ) {
        this.idMapping = idMapping;
        this.orientation = orientation;
        this.adjacencyListWithPropertiesBuilder = adjacencyListWithPropertiesBuilder;
        this.importerBuilder = importerBuilder;
        this.relationshipCounter = relationshipCounter;
        this.loadRelationshipProperty = loadRelationshipProperty;
        this.isMultiGraph = isMultiGraph;
        this.concurrency = concurrency;
        this.executorService = executorService;

        this.threadLocalBuilders = AutoCloseableThreadLocal.withInitial(() -> new ThreadLocalBuilder(
            idMapping,
            importerBuilder,
            bufferSize,
            propertyKeyIds
        ));
    }

    public void add(long source, long target) {
        addFromInternal(idMapping.unsafeToMappedNodeId(source), idMapping.unsafeToMappedNodeId(target));
    }

    public void add(long source, long target, double relationshipPropertyValue) {
        addFromInternal(
            idMapping.unsafeToMappedNodeId(source),
            idMapping.unsafeToMappedNodeId(target),
            relationshipPropertyValue
        );
    }

    public void add(long source, long target, double[] relationshipPropertyValues) {
        addFromInternal(
            idMapping.unsafeToMappedNodeId(source),
            idMapping.unsafeToMappedNodeId(target),
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
        threadLocalBuilders.get().addRelationship(idMapping.toRootNodeId(source), idMapping.toRootNodeId(target));
    }

    public void addFromInternal(long source, long target, double relationshipPropertyValue) {
        threadLocalBuilders.get().addRelationship(
            idMapping.toRootNodeId(source),
            idMapping.toRootNodeId(target),
            relationshipPropertyValue
        );
    }

    public void addFromInternal(long source, long target, double[] relationshipPropertyValues) {
        threadLocalBuilders.get().addRelationship(
            idMapping.toRootNodeId(source),
            idMapping.toRootNodeId(target),
            relationshipPropertyValues
        );
    }

    public Relationships build() {
        return buildAll().get(0);
    }

    public List<Relationships> buildAll() {
        threadLocalBuilders.close();

        importerBuilder.prepareFlushTasks();
        var flushTasks = importerBuilder.flushTasks().collect(Collectors.toList());

        ParallelUtil.runWithConcurrency(concurrency, flushTasks, executorService);

        var adjacencyListsWithProperties = adjacencyListWithPropertiesBuilder.build();
        var adjacencyList = adjacencyListsWithProperties.adjacency();

        if (loadRelationshipProperty) {
            return adjacencyListsWithProperties.properties().stream().map(compressedProperties ->
                Relationships.of(
                    relationshipCounter.longValue(),
                    orientation,
                    isMultiGraph,
                    adjacencyList,
                    compressedProperties,
                    DefaultValue.DOUBLE_DEFAULT_FALLBACK
                )
            ).collect(Collectors.toList());
        } else {
            return List.of(Relationships.of(
                relationshipCounter.longValue(),
                orientation,
                isMultiGraph,
                adjacencyList
            ));
        }
    }

    private static class ThreadLocalBuilder implements AutoCloseable {

        private final SingleTypeRelationshipImporter importer;
        private final RelationshipPropertiesBatchBuffer propertiesBatchBuffer;
        private final int[] propertyKeyIds;

        private int localRelationshipId;

        ThreadLocalBuilder(
            IdMapping idMap,
            SingleTypeRelationshipImporter.Builder.WithImporter importerBuilder,
            int bufferSize,
            int[] propertyKeyIds
        ) {
            this.propertyKeyIds = propertyKeyIds;

            if (propertyKeyIds.length > 1) {
                this.propertiesBatchBuffer = new RelationshipPropertiesBatchBuffer(bufferSize, propertyKeyIds.length);
                this.importer = importerBuilder.withBuffer(idMap, bufferSize, propertiesBatchBuffer);
            } else {
                this.propertiesBatchBuffer = null;
                this.importer = importerBuilder.withBuffer(
                    idMap,
                    bufferSize,
                    RelationshipImporter.preLoadedPropertyReader()
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
                propertiesBatchBuffer.add(nextRelationshipId, keyIds[i], relationshipPropertyValues[i]);
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
