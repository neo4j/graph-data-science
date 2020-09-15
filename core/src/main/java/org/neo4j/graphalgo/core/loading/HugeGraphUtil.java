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
import com.carrotsearch.hppc.IntObjectHashMap;
import org.neo4j.graphalgo.AbstractRelationshipProjection;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public final class HugeGraphUtil {

    private static final String DUMMY_PROPERTY = "property";

    private HugeGraphUtil() {}

    public static IdMapBuilder idMapBuilder(
        long maxOriginalId,
        boolean hasLabelInformation,
        int concurrency,
        AllocationTracker tracker
    ) {
        return new IdMapBuilder(
            maxOriginalId,
            hasLabelInformation,
            concurrency,
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
        return create(
            idMap,
            Collections.emptyMap(),
            relationships,
            tracker
        );
    }

    public static HugeGraph create(IdMap idMap, Map<String, NodeProperties> nodeProperties, Relationships relationships, AllocationTracker tracker) {
        return HugeGraph.create(
            idMap,
            nodeProperties,
            relationships.topology(),
            relationships.properties(),
            tracker
        );
    }

    public static class IdMapBuilder {

        private final HugeLongArrayBuilder idMapBuilder;
        private final NodesBatchBuffer buffer;
        private final HugeNodeImporter nodeImporter;
        private final BitSet seenIds;

        private final AtomicInteger nextLabelId;
        private final Map<NodeLabel, Integer> elementIdentifierLabelTokenMapping;
        private final IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping;

        private final long maxOriginalId;
        private final int concurrency;
        private final AllocationTracker tracker;

        IdMapBuilder(
            long maxOriginalId,
            boolean hasLabelInformation,
            int concurrency,
            AllocationTracker tracker
        ) {
            this.maxOriginalId = maxOriginalId;
            this.concurrency = concurrency;
            this.tracker = tracker;

            this.idMapBuilder = HugeLongArrayBuilder.of(maxOriginalId + 1, tracker);

            this.nextLabelId = new AtomicInteger(0);
            this.elementIdentifierLabelTokenMapping = new HashMap<>();
            this.labelTokenNodeLabelMapping = new IntObjectHashMap<>();

            this.nodeImporter = new HugeNodeImporter(idMapBuilder, new HashMap<>(), labelTokenNodeLabelMapping);

            this.seenIds = new BitSet();

            this.buffer = new NodesBatchBufferBuilder()
                .capacity(ParallelUtil.DEFAULT_BATCH_SIZE)
                .hasLabelInformation(hasLabelInformation)
                .readProperty(false)
                .build();
        }

        public void addNode(long originalId, NodeLabel... nodeLabels) {
            if (seenIds.get(originalId)) {
                return;
            }
            seenIds.set(originalId);

            long[] labels = labelTokens(nodeLabels);

            buffer.add(originalId, -1, labels);

            if (buffer.isFull()) {
                flushBuffer();
                reset();
            }
        }

        public IdMap build() {
            flushBuffer();

            return org.neo4j.graphalgo.core.loading.IdMapBuilder.build(
                idMapBuilder,
                nodeImporter.nodeLabelBitSetMapping,
                maxOriginalId,
                concurrency,
                tracker
            );
        }

        private long[] labelTokens(NodeLabel... nodeLabels) {
            long[] labelIds = new long[nodeLabels.length];

            for (int i = 0; i < nodeLabels.length; i++) {
                NodeLabel nodeLabel = nodeLabels[i];
                long labelId = elementIdentifierLabelTokenMapping.computeIfAbsent(nodeLabel, (l) -> {
                    int nextLabelId = this.nextLabelId.getAndIncrement();
                    labelTokenNodeLabelMapping.put(nextLabelId, Collections.singletonList(nodeLabel));
                    return nextLabelId;
                });
                labelIds[i] = labelId;
            }

            return labelIds;
        }

        private void flushBuffer() {
            this.nodeImporter.importNodes(buffer, (nodeReference, labelIds, propertiesReference, internalId) -> 0);
        }

        private void reset() {
            buffer.reset();
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
            addFromInternal(idMapping.toMappedNodeId(source), idMapping.toMappedNodeId(target), relationshipPropertyValue);
        }

        public <T extends Relationship> void add(Stream<T> relationshipStream) {
            relationshipStream.forEach(this::add);
        }

        public synchronized <T extends Relationship>  void add(T relationship) {
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
