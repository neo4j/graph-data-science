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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.AbstractRelationshipProjection;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.GraphStoreSchema;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.SetBitsIterable;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

;

public final class HugeGraphUtil {

    public static final String DUMMY_PROPERTY = "property";

    private HugeGraphUtil() {}

    public static IdMapBuilder idMapBuilder(
        long maxOriginalId,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        return new IdMapBuilder(
            maxOriginalId,
            executorService,
            tracker
        );
    }

    public static RelationshipsBuilder createRelImporter(
        IdMap idMap,
        Orientation orientation,
        boolean loadRelationshipProperty,
        Aggregation aggregation,
        boolean preAggregate,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        return new RelationshipsBuilder(
            idMap,
            orientation,
            loadRelationshipProperty,
            aggregation,
            preAggregate,
            executorService,
            tracker
        );
    }

    public static HugeGraph create(IdMap idMap, Relationships relationships, AllocationTracker tracker) {
        var nodeSchemaBuilder = NodeSchema.builder();
        idMap.availableNodeLabels().forEach(nodeSchemaBuilder::addEmptyMapForLabelWithoutProperties);
        return create(
            idMap,
            nodeSchemaBuilder.build(),
            Collections.emptyMap(),
            relationships,
            tracker
        );
    }

    public static HugeGraph create(IdMap idMap, Map<String, NodeProperties> nodeProperties, Relationships relationships, AllocationTracker tracker) {
        if (nodeProperties.isEmpty()) {
            return create(idMap, relationships, tracker);
        } else {
            var nodeSchemaBuilder = NodeSchema.builder();
            nodeProperties.forEach((propertyName, property) -> nodeSchemaBuilder.addPropertyAndTypeForLabel(
                NodeLabel.ALL_NODES,
                propertyName,
                property.valueType()
            ));
            return create(
                idMap,
                nodeSchemaBuilder.build(),
                nodeProperties,
                relationships,
                tracker
            );
        }
    }

    public static HugeGraph create(
        IdMap idMap,
        NodeSchema nodeSchema,
        Map<String, NodeProperties> nodeProperties,
        Relationships relationships,
        AllocationTracker tracker
    ) {
        var relationshipSchemaBuilder = RelationshipSchema.builder();
        if (relationships.properties().isPresent()) {
            relationshipSchemaBuilder.addPropertyAndTypeForRelationshipType(
                RelationshipType.of("REL"),
                "property",
                ValueType.DOUBLE
            );
        } else {
            relationshipSchemaBuilder.addEmptyMapForRelationshipTypeWithoutProperties(RelationshipType.of("REL"));
        }

        return HugeGraph.create(
            idMap,
            GraphStoreSchema.of(nodeSchema, relationshipSchemaBuilder.build()),
            nodeProperties,
            relationships.topology(),
            relationships.properties(),
            tracker
        );
    }

    public static class IdMapBuilder {

        final AllocationTracker tracker;
        final ExecutorService executorService;

        private final BitSet seenOriginalIds;
        private final HugeSparseLongArray.Builder originalToInternalBuilder;
        private final Map<NodeLabel, BitSet> labelInformation;

        private long nextAvailableId;
        private IdMap idMap;

        IdMapBuilder(
            long maxOriginalId,
            ExecutorService executorService,
            AllocationTracker tracker
        ) {
            this.executorService = executorService;
            this.tracker = tracker;

            this.originalToInternalBuilder = HugeSparseLongArray.Builder.create(maxOriginalId + 1, tracker);
            this.nextAvailableId = 0;
            this.seenOriginalIds = new BitSet(maxOriginalId);
            this.labelInformation = new HashMap<>();
        }

        public void addNode(long originalId) {
            if (idMap != null) {
                throw new UnsupportedOperationException("Cannot add new nodes after `idMap` has been called");
            }

            if (!seenOriginalIds.get(originalId)) {
                originalToInternalBuilder.set(originalId, nextAvailableId++);
                seenOriginalIds.set(originalId);
            }
        }

        public void addNode(long originalId, NodeLabel... nodeLabels) {
            if (idMap != null) {
                throw new UnsupportedOperationException("Cannot add new nodes after `idMap` has been called");
            }
            if (!seenOriginalIds.get(originalId)) {
                long internalNodeId = nextAvailableId++;
                originalToInternalBuilder.set(originalId, internalNodeId);
                seenOriginalIds.set(originalId);

                for (NodeLabel nodeLabel : nodeLabels) {
                    labelInformation
                        .computeIfAbsent(nodeLabel, (ignore) -> new BitSet(seenOriginalIds.size()))
                        .set(internalNodeId);
                }
            }
        }

        public IdMap build() {
            if (idMap == null) {
                HugeSparseLongArray originalToInternal = originalToInternalBuilder.build();

                HugeLongArray internalToNeo = HugeLongArray.newArray(nextAvailableId, tracker);
                new SetBitsIterable(seenOriginalIds).forEach(nodeId -> internalToNeo.set(
                    originalToInternal.get(nodeId),
                    nodeId
                ));

                idMap = new IdMap(internalToNeo, originalToInternal, labelInformation, internalToNeo.size());
            }
            return idMap;
        }
    }

    public static class RelationshipsBuilder {

        static final int DUMMY_PROPERTY_ID = -2;
        private final org.neo4j.graphalgo.core.loading.RelationshipsBuilder relationshipsBuilder;
        private final RelationshipImporter relationshipImporter;
        private final RelationshipImporter.Imports imports;
        private final RelationshipsBatchBuffer relationshipBuffer;
        private final IdMapping idMapping;
        private final Orientation orientation;
        private final boolean loadRelationshipProperty;
        private final ExecutorService executorService;
        private final Aggregation aggregation;
        private final LongAdder relationshipCounter;

        public RelationshipsBuilder(
            IdMapping idMapping,
            Orientation orientation,
            boolean loadRelationshipProperty,
            Aggregation aggregation,
            boolean preAggregate,
            ExecutorService executorService,
            AllocationTracker tracker
        ) {
            this.orientation = orientation;
            this.loadRelationshipProperty = loadRelationshipProperty;
            this.executorService = executorService;
            this.idMapping = idMapping;
            this.aggregation = aggregation;
            this.relationshipCounter = new LongAdder();

            ImportSizing importSizing = ImportSizing.of(1, idMapping.nodeCount());
            int pageSize = importSizing.pageSize();
            int numberOfPages = importSizing.numberOfPages();

            int[] propertyKeyIds = loadRelationshipProperty ? new int[]{DUMMY_PROPERTY_ID} : new int[0];
            double[] defaultValues = loadRelationshipProperty ? new double[]{Double.NaN} : new double[0];

            AbstractRelationshipProjection.Builder projectionBuilder = RelationshipProjection
                .builder()
                .type("*")
                .orientation(orientation);

            if (loadRelationshipProperty) {
                projectionBuilder.addProperty(DUMMY_PROPERTY, DUMMY_PROPERTY, DefaultValue.DEFAULT, aggregation);
            }

            this.relationshipsBuilder = new org.neo4j.graphalgo.core.loading.RelationshipsBuilder(
                projectionBuilder.build(),
                TransientAdjacencyListBuilder.builderFactory(tracker),
                TransientAdjacencyOffsets.forPageSize(pageSize)
            );

            AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
                relationshipsBuilder,
                numberOfPages,
                pageSize,
                tracker,
                relationshipCounter,
                propertyKeyIds,
                defaultValues,
                new Aggregation[]{aggregation},
                preAggregate
            );

            this.relationshipImporter = new RelationshipImporter(tracker, adjacencyBuilder);
            this.imports = relationshipImporter.imports(orientation, loadRelationshipProperty);
            this.relationshipBuffer = new RelationshipsBatchBuffer(idMapping, -1, ParallelUtil.DEFAULT_BATCH_SIZE);
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

        public <T extends Relationship> void add(Stream<T> relationshipStream) {
            relationshipStream.forEach(this::add);
        }

        public synchronized <T extends Relationship> void add(T relationship) {
            add(relationship.sourceNodeId(), relationship.targetNodeId(), relationship.property());
        }

        public void addFromInternal(long source, long target) {
            relationshipBuffer.add(source, target, -1L, -1L);
            if (relationshipBuffer.isFull()) {
                flushBuffer();
                relationshipBuffer.reset();
            }
        }

        public void addFromInternal(long source, long target, double relationshipPropertyValue) {
            relationshipBuffer.add(source, target, -1L, Double.doubleToLongBits(relationshipPropertyValue));
            if (relationshipBuffer.isFull()) {
                flushBuffer();
                relationshipBuffer.reset();
            }
        }

        public <T extends Relationship> void addFromInternal(Stream<T> relationshipStream) {
            relationshipStream.forEach(this::addFromInternal);
        }

        public synchronized <T extends Relationship> void addFromInternal(T relationship) {
            addFromInternal(relationship.sourceNodeId(), relationship.targetNodeId(), relationship.property());
        }

        public Relationships build() {
            flushBuffer();

            ParallelUtil.run(relationshipImporter.flushTasks(), executorService);
            return Relationships.of(
                relationshipCounter.longValue(),
                orientation,
                Aggregation.equivalentToNone(aggregation),
                relationshipsBuilder.adjacencyList(),
                relationshipsBuilder.globalAdjacencyOffsets(),
                loadRelationshipProperty ? relationshipsBuilder.properties() : null,
                loadRelationshipProperty ? relationshipsBuilder.globalPropertyOffsets() : null,
                Double.NaN
            );
        }

        private void flushBuffer() {
            RelationshipImporter.PropertyReader propertyReader = loadRelationshipProperty ? RelationshipImporter.preLoadedPropertyReader() : null;

            imports.importRelationships(relationshipBuffer, propertyReader);
            relationshipBuffer.reset();
        }
    }

    public interface Relationship {
        long sourceNodeId();

        long targetNodeId();

        double property();
    }
}
