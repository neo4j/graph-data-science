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
package org.neo4j.graphalgo.core.loading.construction;

import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;
import org.neo4j.graphalgo.core.loading.AdjacencyBuilder;
import org.neo4j.graphalgo.core.loading.AdjacencyListWithPropertiesBuilder;
import org.neo4j.graphalgo.core.loading.ImportSizing;
import org.neo4j.graphalgo.core.loading.RecordsBatchBuffer;
import org.neo4j.graphalgo.core.loading.RelationshipImporter;
import org.neo4j.graphalgo.core.loading.RelationshipPropertiesBatchBuffer;
import org.neo4j.graphalgo.core.loading.SingleTypeRelationshipImporter;
import org.neo4j.graphalgo.core.loading.TransientAdjacencyListBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.utils.AutoCloseableThreadLocal;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.api.DefaultValue.DOUBLE_DEFAULT_FALLBACK;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

public class RelationshipsBuilder {

    private final AdjacencyListWithPropertiesBuilder adjacencyListWithPropertiesBuilder;
    private final SingleTypeRelationshipImporter.Builder.WithImporter importerBuilder;

    private final IdMapping idMapping;
    private final Orientation orientation;
    private final int concurrency;
    private final ExecutorService executorService;
    private final LongAdder relationshipCounter;
    private final AutoCloseableThreadLocal<ThreadLocalBuilder> threadLocalBuilders;
    private final boolean loadRelationshipProperty;
    private final boolean isMultiGraph;

    public RelationshipsBuilder(
        IdMapping idMapping,
        Orientation orientation,
        List<GraphFactory.PropertyConfig> propertyConfigs,
        boolean preAggregate,
        int concurrency,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        this.idMapping = idMapping;
        this.orientation = orientation;
        this.loadRelationshipProperty = !propertyConfigs.isEmpty();

        var aggregations = propertyConfigs.isEmpty()
            ? new Aggregation[]{Aggregation.NONE}
            : propertyConfigs.stream()
                .map(GraphFactory.PropertyConfig::aggregation)
                .map(Aggregation::resolve)
                .toArray(Aggregation[]::new);

        this.isMultiGraph = Arrays.stream(aggregations).allMatch(Aggregation::equivalentToNone);

        this.concurrency = concurrency;
        this.executorService = executorService;

        this.relationshipCounter = new LongAdder();

        // TODO: configurable?
        var relationshipType = RelationshipType.ALL_RELATIONSHIPS;
        var importSizing = ImportSizing.of(concurrency, idMapping.rootNodeCount());
        int pageSize = importSizing.pageSize();
        int numberOfPages = importSizing.numberOfPages();
        int bufferSize = (int) Math.min(idMapping.rootNodeCount(), RecordsBatchBuffer.DEFAULT_BUFFER_SIZE);

        var projectionBuilder = RelationshipProjection
            .builder()
            .type(relationshipType.name())
            .orientation(orientation);

        propertyConfigs.forEach(propertyConfig -> projectionBuilder.addProperty(
            GraphFactory.DUMMY_PROPERTY,
            GraphFactory.DUMMY_PROPERTY,
            DefaultValue.DEFAULT,
            propertyConfig.aggregation()
        ));

        var projection = projectionBuilder.build();

        int[] propertyKeyIds = IntStream.range(0, propertyConfigs.size()).toArray();
        // TODO: configurable?
        double[] defaultValues = propertyConfigs.stream().mapToDouble(ignored -> Double.NaN).toArray();

        this.adjacencyListWithPropertiesBuilder = AdjacencyListWithPropertiesBuilder.create(
            idMapping.rootNodeCount(),
            projection,
            TransientAdjacencyListBuilder.builderFactory(tracker),
            TransientAdjacencyOffsets.forPageSize(pageSize),
            aggregations,
            propertyKeyIds,
            defaultValues,
            tracker
        );

        AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
            adjacencyListWithPropertiesBuilder,
            numberOfPages,
            pageSize,
            tracker,
            relationshipCounter,
            preAggregate
        );

        var relationshipImporter = new RelationshipImporter(tracker, adjacencyBuilder);

        this.importerBuilder = new SingleTypeRelationshipImporter.Builder(
            relationshipType,
            projection,
            loadRelationshipProperty,
            NO_SUCH_RELATIONSHIP_TYPE,
            relationshipImporter,
            relationshipCounter,
            false
        ).loadImporter(loadRelationshipProperty);

        this.threadLocalBuilders = AutoCloseableThreadLocal.withInitial(() -> new ThreadLocalBuilder(
            idMapping,
            importerBuilder,
            bufferSize,
            propertyKeyIds.length,
            propertyKeyIds
        ));
    }

    public void add(long source, long target) {
        addFromInternal(idMapping.toMappedNodeId(source), idMapping.toMappedNodeId(target));
    }

    public void add(long source, long target, double relationshipPropertyValue) {
        addFromInternal(
            idMapping.toMappedNodeId(source),
            idMapping.toMappedNodeId(target),
            relationshipPropertyValue
        );
    }

    public void add(long source, long target, double[] relationshipPropertyValues) {
        addFromInternal(
            idMapping.toMappedNodeId(source),
            idMapping.toMappedNodeId(target),
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

        var flushTasks = importerBuilder.flushTasks().collect(Collectors.toList());

        ParallelUtil.runWithConcurrency(concurrency, flushTasks, executorService);

        var adjacencyListsWithProperties = adjacencyListWithPropertiesBuilder.build();
        var compressedTopology = adjacencyListsWithProperties.adjacency();

        if (loadRelationshipProperty) {
            return adjacencyListsWithProperties.properties().stream().map(compressedProperties ->
                Relationships.of(
                    relationshipCounter.longValue(),
                    orientation,
                    isMultiGraph,
                    compressedTopology.adjacencyList(),
                    compressedTopology.adjacencyOffsets(),
                    compressedProperties.adjacencyList(),
                    compressedProperties.adjacencyOffsets(),
                    DOUBLE_DEFAULT_FALLBACK
                )
            ).collect(Collectors.toList());
        } else {
            return List.of(Relationships.of(
                relationshipCounter.longValue(),
                orientation,
                isMultiGraph,
                compressedTopology.adjacencyList(),
                compressedTopology.adjacencyOffsets()
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
            int propertyCount,
            int[] propertyKeyIds
        ) {
            this.propertyKeyIds = propertyKeyIds;

            if (propertyKeyIds.length > 1) {
                this.propertiesBatchBuffer = new RelationshipPropertiesBatchBuffer(bufferSize, propertyCount);
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
            importer.buffer().add(source, target, NO_SUCH_RELATIONSHIP);
            if (importer.buffer().isFull()) {
                flushBuffer();
            }
        }

        void addRelationship(long source, long target, double relationshipPropertyValue) {
            importer
                .buffer()
                .add(source, target, NO_SUCH_RELATIONSHIP, Double.doubleToLongBits(relationshipPropertyValue));
            if (importer.buffer().isFull()) {
                flushBuffer();
            }
        }

        void addRelationship(long source, long target, double[] relationshipPropertyValues) {
            int nextRelationshipId = localRelationshipId++;
            importer.buffer().add(source, target, NO_SUCH_RELATIONSHIP, nextRelationshipId);
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
