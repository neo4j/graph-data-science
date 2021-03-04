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

import org.neo4j.graphalgo.AbstractRelationshipProjection;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;
import org.neo4j.graphalgo.core.loading.AdjacencyBuilder;
import org.neo4j.graphalgo.core.loading.AdjacencyListWithPropertiesBuilder;
import org.neo4j.graphalgo.core.loading.ImportSizing;
import org.neo4j.graphalgo.core.loading.RelationshipImporter;
import org.neo4j.graphalgo.core.loading.RelationshipsBatchBuffer;
import org.neo4j.graphalgo.core.loading.TransientAdjacencyListBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.utils.AutoCloseableThreadLocal;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.api.DefaultValue.DOUBLE_DEFAULT_FALLBACK;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

public class RelationshipsBuilder {

    private static final int DUMMY_PROPERTY_ID = -2;

    private final AdjacencyListWithPropertiesBuilder adjacencyListWithPropertiesBuilder;
    private final RelationshipImporter relationshipImporter;
    private final RelationshipImporter.Imports imports;
    private final IdMapping idMapping;
    private final Orientation orientation;
    private final int concurrency;
    private final ExecutorService executorService;
    private final Aggregation aggregation;
    private final LongAdder relationshipCounter;
    private final AutoCloseableThreadLocal<ThreadLocalBuilder> threadLocalBuilders;

    public RelationshipsBuilder(
        IdMapping idMapping,
        Orientation orientation,
        boolean loadRelationshipProperty,
        Aggregation aggregation,
        boolean preAggregate,
        int concurrency,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        this.idMapping = idMapping;
        this.orientation = orientation;
        this.aggregation = aggregation;
        this.concurrency = concurrency;
        this.executorService = executorService;

        this.relationshipCounter = new LongAdder();

        ImportSizing importSizing = ImportSizing.of(concurrency, idMapping.rootNodeCount());
        int pageSize = importSizing.pageSize();
        int numberOfPages = importSizing.numberOfPages();

        int[] propertyKeyIds = loadRelationshipProperty ? new int[]{DUMMY_PROPERTY_ID} : new int[0];
        double[] defaultValues = loadRelationshipProperty ? new double[]{Double.NaN} : new double[0];

        AbstractRelationshipProjection.Builder projectionBuilder = RelationshipProjection
            .builder()
            .type("*")
            .orientation(orientation);

        if (loadRelationshipProperty) {
            projectionBuilder.addProperty(GraphFactory.DUMMY_PROPERTY, GraphFactory.DUMMY_PROPERTY, DefaultValue.DEFAULT, aggregation);
        }

        this.adjacencyListWithPropertiesBuilder = AdjacencyListWithPropertiesBuilder.create(
            idMapping.rootNodeCount(),
            projectionBuilder.build(),
            TransientAdjacencyListBuilder.builderFactory(tracker),
            TransientAdjacencyOffsets.forPageSize(pageSize),
            new Aggregation[]{aggregation},
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

        this.relationshipImporter = new RelationshipImporter(tracker, adjacencyBuilder);
        this.imports = relationshipImporter.imports(orientation, loadRelationshipProperty);

        this.threadLocalBuilders = AutoCloseableThreadLocal.withInitial(() -> new ThreadLocalBuilder(
            idMapping,
            imports,
            loadRelationshipProperty
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

        ParallelUtil.runWithConcurrency(concurrency, relationshipImporter.flushTasks(), executorService);

        var build = adjacencyListWithPropertiesBuilder.build();
        var compressedTopology = build.adjacency();
        var compressedProperties = build.properties().stream().findFirst().orElse(null);

        var relationships = Relationships.of(
            relationshipCounter.longValue(),
            orientation,
            Aggregation.equivalentToNone(aggregation),
            compressedTopology.adjacencyList(),
            compressedTopology.adjacencyOffsets(),
            compressedProperties != null ? compressedProperties.adjacencyList() : null,
            compressedProperties != null ? compressedProperties.adjacencyOffsets() : null,
            DOUBLE_DEFAULT_FALLBACK
        );
        return List.of(relationships);
    }

    private static class ThreadLocalBuilder implements AutoCloseable {

        private final RelationshipsBatchBuffer relationshipBuffer;
        private final RelationshipImporter.Imports imports;
        private final boolean loadRelationshipProperty;

        ThreadLocalBuilder(
            IdMapping idMap,
            RelationshipImporter.Imports imports,
            boolean loadRelationshipProperty
        ) {
            this.relationshipBuffer = new RelationshipsBatchBuffer(idMap, NO_SUCH_RELATIONSHIP_TYPE, ParallelUtil.DEFAULT_BATCH_SIZE);
            this.imports = imports;
            this.loadRelationshipProperty = loadRelationshipProperty;
        }

        void addRelationship(long source, long target) {
            relationshipBuffer.add(source, target, NO_SUCH_PROPERTY_KEY);
            if (relationshipBuffer.isFull()) {
                flushBuffer();
                relationshipBuffer.reset();
            }
        }

        void addRelationship(long source, long target, double relationshipPropertyValue) {
            relationshipBuffer.add(source, target, NO_SUCH_PROPERTY_KEY, Double.doubleToLongBits(relationshipPropertyValue));
            if (relationshipBuffer.isFull()) {
                flushBuffer();
                relationshipBuffer.reset();
            }
        }

        void addRelationship(long source, long target, double[] relationshipPropertyValue) {
            for (double propertyValue : relationshipPropertyValue) {
               addRelationship(source, target, propertyValue);
            }
        }

        private void flushBuffer() {
            RelationshipImporter.PropertyReader propertyReader = loadRelationshipProperty ? RelationshipImporter.preLoadedPropertyReader() : null;

            imports.importRelationships(relationshipBuffer, propertyReader);
            relationshipBuffer.reset();
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
