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
package org.neo4j.gds.core.io;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.graph.GraphProperty;
import org.neo4j.gds.compat.CompatInput;
import org.neo4j.gds.compat.CompatPropertySizeCalculator;
import org.neo4j.gds.compat.InputEntityIdVisitor;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.io.GraphStoreExporter.IdMapFunction;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.cache.idmapping.string.LongEncoder;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.id.IdValidator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class GraphStoreInput implements CompatInput {

    private final MetaDataStore metaDataStore;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;

    private final Set<GraphProperty> graphProperties;
    private final int batchSize;
    private final int concurrency;
    private final IdMapFunction idMapFunction;
    private final IdMode idMode;
    private final Capabilities capabilities;

    enum IdMode implements Supplier<InputEntityIdVisitor.Long> {
        MAPPING(IdType.INTEGER, new Groups()) {
            @Override
            public InputEntityIdVisitor.Long get() {
                return Neo4jProxy.inputEntityLongIdVisitor(IdType.INTEGER);
            }
        },
        ACTUAL(IdType.ACTUAL, Groups.EMPTY) {
            @Override
            public InputEntityIdVisitor.Long get() {
                return Neo4jProxy.inputEntityLongIdVisitor(IdType.ACTUAL);
            }
        };

        private final IdType idType;
        private final ReadableGroups readableGroups;

        IdMode(IdType idType, ReadableGroups readableGroups) {
            this.idType = idType;
            this.readableGroups = readableGroups;
        }
    }

    public static GraphStoreInput of(
        MetaDataStore metaDataStore,
        NodeStore nodeStore,
        RelationshipStore relationshipStore,
        Capabilities capabilities,
        Set<GraphProperty> graphProperties,
        int batchSize,
        int concurrency,
        GraphStoreExporter.IdMappingType idMappingType
    ) {
        // Neo reserves node id 2^32 - 1 for handling special internal cases.
        // If our id space is below that value, we can use actual mapping, i.e.,
        // we directly forward the internal GDS ids to the batch importer.
        // If the GDS ids contain the reserved id, we need to fall back to Neo's
        // id mapping functionality. This however, is limited to external ids up
        // until 2^58, which is why we need to ensure that we don't exceed that.

        // This check is correct for now but will not work if Neo adds
        // more reserved ids. Their API in `IdValidator` seems to be
        // prepared for more reserved ids. The only other option would
        // be to scan through all original ids in the id map and
        // perform individual checks with `IdValidator#isReservedId`.
        if (idMappingType.highestId(nodeStore.idMap) >= IdValidator.INTEGER_MINUS_ONE
            && idMappingType.contains(nodeStore.idMap, IdValidator.INTEGER_MINUS_ONE)) {
            try {
                // We try to encode the highest mapped neo id in order to check if we
                // exceed the limit. This is the encoder used when using IdType.INTEGER
                new LongEncoder().encode(idMappingType.highestId(nodeStore.idMap));
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("The range of original ids specified in the graph exceeds the limit", e);
            }
            return new GraphStoreInput(
                metaDataStore,
                nodeStore,
                relationshipStore,
                capabilities,
                graphProperties,
                batchSize,
                concurrency,
                idMappingType,
                IdMode.MAPPING
            );
        } else {
            return new GraphStoreInput(
                metaDataStore,
                nodeStore,
                relationshipStore,
                capabilities,
                graphProperties,
                batchSize,
                concurrency,
                idMappingType,
                IdMode.ACTUAL
            );
        }
    }

    private GraphStoreInput(
        MetaDataStore metaDataStore,
        NodeStore nodeStore,
        RelationshipStore relationshipStore,
        Capabilities capabilities,
        Set<GraphProperty> graphProperties,
        int batchSize,
        int concurrency,
        IdMapFunction idMapFunction,
        IdMode idMode
    ) {
        this.metaDataStore = metaDataStore;
        this.nodeStore = nodeStore;
        this.relationshipStore = relationshipStore;
        this.graphProperties = graphProperties;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.idMapFunction = idMapFunction;
        this.idMode = idMode;
        this.capabilities = capabilities;
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        return () -> new NodeImporter(nodeStore, batchSize, idMode.get(), idMapFunction);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return () -> new RelationshipImporter(relationshipStore, batchSize, idMode.get(), idMapFunction);
    }

    @Override
    public IdType idType() {
        return idMode.idType;
    }

    @Override
    public ReadableGroups groups() {
        return idMode.readableGroups;
    }

    @Override
    public Input.Estimates calculateEstimates(CompatPropertySizeCalculator propertySizeCalculator) {
        long numberOfNodeProperties = nodeStore.propertyCount();
        long numberOfRelationshipProperties = relationshipStore.propertyCount();

        return Input.knownEstimates(
            nodeStore.nodeCount,
            relationshipStore.relationshipCount,
            numberOfNodeProperties,
            numberOfRelationshipProperties,
            numberOfNodeProperties * Double.BYTES,
            numberOfRelationshipProperties * Double.BYTES,
            nodeStore.labelCount()
        );
    }

    public MetaDataStore metaDataStore() {
        return metaDataStore;
    }

    public Capabilities capabilities() {
        return capabilities;
    }

    public InputIterable graphProperties() {
        return () -> new GraphPropertyIterator(graphProperties.iterator(), concurrency);
    }

    static class GraphPropertyIterator implements InputIterator {

        private final Iterator<GraphProperty> graphPropertyIterator;
        private final int concurrency;
        private final Queue<Spliterator<?>> splits;
        private @Nullable String currentPropertyName;

        GraphPropertyIterator(Iterator<GraphProperty> graphPropertyIterator, int concurrency) {
            this.graphPropertyIterator = graphPropertyIterator;
            this.concurrency = concurrency;
            this.splits = new ArrayBlockingQueue<>(concurrency);
        }

        @Override
        public InputChunk newChunk() {
            return new GraphPropertyInputChunk();
        }

        @Override
        public synchronized boolean next(InputChunk chunk) throws IOException {
            if (this.splits.isEmpty()) {
                if (this.graphPropertyIterator.hasNext()) {
                    initializeSplits();
                } else {
                    return false;
                }
            }

            if (!this.splits.isEmpty()) {
                ((GraphPropertyInputChunk) chunk).initialize(
                    Objects.requireNonNull(currentPropertyName),
                    this.splits.poll()
                );
                return true;
            }

            this.currentPropertyName = null;
            return false;
        }

        @Override
        public void close() throws IOException {

        }

        private void initializeSplits() {
            var graphProperty = graphPropertyIterator.next();
            var graphPropertySpliterator = graphProperty.values().objects().parallel().spliterator();

            precomputeSplits(graphPropertySpliterator, concurrency);
            this.currentPropertyName = graphProperty.key();
        }

        private void precomputeSplits(Spliterator<?> root, int capacity) {
            var originalCapacity = capacity;
            var queue = new ArrayDeque<Spliterator<?>>();
            queue.add(root);
            capacity--;

            while (!queue.isEmpty() && capacity > 0) {
                var spliterator = queue.poll();

                var split = spliterator.trySplit();

                if (split != null) {
                    queue.offer(spliterator);
                    queue.offer(split);
                    capacity--;
                } else {
                    splits.add(spliterator);
                }
            }

            addRemainingSplits(originalCapacity, queue);
        }

        private void addRemainingSplits(int capacity, Iterable<Spliterator<?>> queue) {
            var queueIterator = queue.iterator();
            for (int i = splits.size(); i < capacity && queueIterator.hasNext(); i++) {
                splits.add(queueIterator.next());
            }
        }
    }

    static class GraphPropertyInputChunk implements InputChunk {

        private String propertyName;
        private Spliterator<?> propertyValues;

        void initialize(String propertyName, Spliterator<?> propertyValues) {
            this.propertyName = propertyName;
            this.propertyValues = propertyValues;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            if (propertyValues.tryAdvance(value -> visitor.property(propertyName, value))) {
                visitor.endOfEntity();
                return true;
            }
            return false;
        }

        @Override
        public void close() throws IOException {

        }
    }

    abstract static class GraphImporter implements InputIterator {

        private final long nodeCount;
        private final int batchSize;
        final InputEntityIdVisitor.Long inputEntityIdVisitor;
        final IdMapFunction idMapFunction;

        private long id;

        GraphImporter(
            long nodeCount,
            int batchSize,
            InputEntityIdVisitor.Long inputEntityIdVisitor,
            IdMapFunction idMapFunction
        ) {
            this.nodeCount = nodeCount;
            this.batchSize = batchSize;
            this.inputEntityIdVisitor = inputEntityIdVisitor;
            this.idMapFunction = idMapFunction;
        }

        @Override
        public synchronized boolean next(InputChunk chunk) {
            if (id >= nodeCount) {
                return false;
            }
            long startId = id;
            id = Math.min(nodeCount, startId + batchSize);

            assert chunk instanceof EntityChunk;
            ((EntityChunk) chunk).initialize(startId, id);
            return true;
        }

        @Override
        public void close() {
        }
    }

    static class NodeImporter extends GraphImporter {

        private final NodeStore nodeStore;

        NodeImporter(
            NodeStore nodeStore,
            int batchSize,
            InputEntityIdVisitor.Long inputEntityIdVisitor,
            IdMapFunction idMapFunction
        ) {
            super(nodeStore.nodeCount, batchSize, inputEntityIdVisitor, idMapFunction);
            this.nodeStore = nodeStore;
        }

        @Override
        public InputChunk newChunk() {
            return new NodeChunk(nodeStore, inputEntityIdVisitor, idMapFunction);
        }
    }

    static class RelationshipImporter extends GraphImporter {

        private final RelationshipStore relationshipStore;

        RelationshipImporter(
            RelationshipStore relationshipStore,
            int batchSize,
            InputEntityIdVisitor.Long inputEntityIdVisitor,
            IdMapFunction idMapFunction
        ) {
            super(relationshipStore.nodeCount, batchSize, inputEntityIdVisitor, idMapFunction);
            this.relationshipStore = relationshipStore;
        }

        @Override
        public InputChunk newChunk() {
            return new RelationshipChunk(relationshipStore.concurrentCopy(), inputEntityIdVisitor, idMapFunction);
        }
    }

    abstract static class EntityChunk implements InputChunk {

        final InputEntityIdVisitor.Long inputEntityIdVisitor;

        long id;
        long endId;

        EntityChunk(InputEntityIdVisitor.Long inputEntityIdVisitor) {
            this.inputEntityIdVisitor = inputEntityIdVisitor;
        }

        void initialize(long startId, long endId) {
            this.id = startId;
            this.endId = endId;
        }

        @Override
        public void close() {
        }
    }

    static class NodeChunk extends EntityChunk {

        private final NodeStore nodeStore;

        private final boolean hasLabels;
        private final boolean hasProperties;
        private final IdMapFunction idMapFunction;

        NodeChunk(
            NodeStore nodeStore,
            InputEntityIdVisitor.Long inputEntityIdVisitor,
            IdMapFunction idMapFunction
        ) {
            super(inputEntityIdVisitor);
            this.nodeStore = nodeStore;
            this.hasLabels = nodeStore.hasLabels();
            this.hasProperties = nodeStore.hasProperties();
            this.idMapFunction = idMapFunction;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            if (id < endId) {
                inputEntityIdVisitor.visitNodeId(visitor, idMapFunction.getId(nodeStore.idMap, id));

                if (hasLabels) {
                    String[] labels = nodeStore.labels(id);
                    visitor.labels(labels);

                    if (hasProperties) {
                        for (var label : labels) {
                            nodeStore.nodeProperties
                                .getOrDefault(label, Map.of())
                                .forEach((propertyKey, properties) -> exportProperty(
                                    visitor,
                                    propertyKey,
                                    properties::getObject
                                ));
                        }
                    }
                } else if (hasProperties) { // no label information, but node properties
                    nodeStore.nodeProperties.forEach((label, nodeProperties) -> nodeProperties.forEach((propertyKey, properties) -> exportProperty(
                        visitor,
                        propertyKey,
                        properties::getObject
                    )));
                }

                nodeStore.additionalProperties.forEach((propertyKey, propertyFn) -> {
                    exportProperty(visitor, propertyKey, propertyFn);
                });

                visitor.endOfEntity();
                id++;
                return true;
            }
            return false;
        }

        private void exportProperty(InputEntityVisitor visitor, String propertyKey, LongFunction<Object> propertyFn) {
            var value = propertyFn.apply(id);
            if (value != null) {
                visitor.property(propertyKey, value);
            }
        }
    }

    static class RelationshipChunk extends EntityChunk {

        private final RelationshipStore relationshipStore;
        private final Map<RelationshipType, RelationshipConsumer> relationshipConsumers;

        RelationshipChunk(
            RelationshipStore relationshipStore,
            InputEntityIdVisitor.Long inputEntityIdVisitor,
            IdMapFunction idMapFunction
        ) {
            super(inputEntityIdVisitor);
            this.relationshipStore = relationshipStore;

            this.relationshipConsumers = relationshipStore
                .relationshipIterators
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new RelationshipConsumer(
                        relationshipStore.idMap(),
                        e.getKey().name,
                        e.getValue().propertyKeys(),
                        inputEntityIdVisitor,
                        idMapFunction
                    )
                ));
        }

        @Override
        public boolean next(InputEntityVisitor visitor) {
            if (id < endId) {
                for (var entry : relationshipStore.relationshipIterators.entrySet()) {
                    var relationshipType = entry.getKey();
                    var relationshipIterator = entry.getValue();
                    var relationshipConsumer = relationshipConsumers.get(relationshipType);
                    relationshipConsumer.setVisitor(visitor);

                    relationshipIterator.forEachRelationship(id, relationshipConsumer);
                }
                id++;
                return true;
            }
            return false;
        }

        private static final class RelationshipConsumer implements CompositeRelationshipIterator.RelationshipConsumer {
            private final IdMap idMap;
            private final String relationshipType;
            private final String[] propertyKeys;
            private final InputEntityIdVisitor.Long inputEntityIdVisitor;
            private final IdMapFunction idMapFunction;
            private InputEntityVisitor visitor;

            private RelationshipConsumer(
                IdMap idMap,
                String relationshipType,
                String[] propertyKeys,
                InputEntityIdVisitor.Long inputEntityIdVisitor,
                IdMapFunction idMapFunction
            ) {
                this.idMap = idMap;
                this.relationshipType = relationshipType;
                this.propertyKeys = propertyKeys;
                this.inputEntityIdVisitor = inputEntityIdVisitor;
                this.idMapFunction = idMapFunction;
            }

            private void setVisitor(InputEntityVisitor visitor) {
                this.visitor = visitor;
            }

            @Override
            public boolean consume(long source, long target, double[] properties) {
                inputEntityIdVisitor.visitSourceId(visitor, idMapFunction.getId(idMap, source));
                inputEntityIdVisitor.visitTargetId(visitor, idMapFunction.getId(idMap, target));
                visitor.type(relationshipType);

                for (int propertyIdx = 0; propertyIdx < propertyKeys.length; propertyIdx++) {
                    visitor.property(
                        propertyKeys[propertyIdx],
                        properties[propertyIdx]
                    );
                }

                try {
                    visitor.endOfEntity();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }

                return true;
            }
        }
    }
}
