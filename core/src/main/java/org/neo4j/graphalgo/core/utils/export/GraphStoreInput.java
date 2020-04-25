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
package org.neo4j.graphalgo.core.utils.export;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.values.storable.Value;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.NodeLabel.ALL_NODES;

public final class GraphStoreInput implements Input {

    private final GraphStore graphStore;

    private final GraphStoreExport.NodeStore nodeStore;

    private final int batchSize;

    GraphStoreInput(GraphStore graphStore, GraphStoreExport.NodeStore nodeStore, int batchSize) {
        this.graphStore = graphStore;
        this.nodeStore = nodeStore;
        this.batchSize = batchSize;
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        return () -> new NodeImporter(nodeStore, batchSize);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return () -> new RelationshipImporter(graphStore, graphStore.nodeCount(), batchSize);
    }

    @Override
    public IdType idType() {
        return IdType.ACTUAL;
    }

    @Override
    public ReadableGroups groups() {
        return Groups.EMPTY;
    }

    @Override
    public Estimates calculateEstimates(ToIntFunction<Value[]> valueSizeCalculator) {
        long numberOfNodeProperties = graphStore.nodePropertyCount();
        long numberOfRelationshipProperties = graphStore.relationshipPropertyCount();

        return Input.knownEstimates(
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            numberOfNodeProperties,
            numberOfRelationshipProperties,
            numberOfNodeProperties * Double.BYTES,
            numberOfRelationshipProperties * Double.BYTES,
            graphStore.nodeLabels().size()
        );
    }

    abstract static class GraphImporter implements InputIterator {

        private final long nodeCount;
        private final int batchSize;

        private long id;

        GraphImporter(long nodeCount, int batchSize) {
            this.nodeCount = nodeCount;
            this.batchSize = batchSize;
        }

        @Override
        public synchronized boolean next(InputChunk chunk) {
            if (id >= nodeCount)
            {
                return false;
            }
            long startId = id;
            id = Math.min(nodeCount, startId + batchSize);

            ((EntityChunk) chunk).initialize(startId, id);
            return true;
        }

        @Override
        public void close() {
        }
    }

    static class NodeImporter extends GraphImporter {

        private final GraphStoreExport.NodeStore nodeStore;

        NodeImporter(GraphStoreExport.NodeStore nodeStore, int batchSize) {
            super(nodeStore.nodeCount, batchSize);
            this.nodeStore = nodeStore;
        }

        @Override
        public InputChunk newChunk() {
            return new NodeChunk(nodeStore);
        }
    }

    static class RelationshipImporter extends GraphImporter {

        private final Map<Pair<RelationshipType, Optional<String>>, RelationshipIterator> relationships;

        RelationshipImporter(GraphStore graphStore, long nodeCount, int batchSize) {
            super(nodeCount, batchSize);
            this.relationships = graphStore
                .relationshipTypes()
                .stream()
                .flatMap(relType -> {
                    Set<String> relProperties = graphStore.relationshipPropertyKeys(relType);
                    if (relProperties.isEmpty()) {
                        return Stream.of(Tuples.pair(relType, Optional.<String>empty()));
                    } else {
                        return relProperties
                            .stream()
                            .map(propertyKey -> Tuples.pair(relType, Optional.of(propertyKey)));
                    }
                })
                .collect(Collectors.toMap(
                    relTypeAndProperty -> relTypeAndProperty,
                    relTypeAndProperty -> graphStore.getGraph(relTypeAndProperty.getOne(), relTypeAndProperty.getTwo())
                ));
        }

        @Override
        public InputChunk newChunk() {
            var concurrentCopies = relationships
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().concurrentCopy()
                ));
            return new RelationshipChunk(concurrentCopies);
        }
    }

    abstract static class EntityChunk implements InputChunk {
        long id;
        long endId;

        void initialize(long startId, long endId) {
            this.id = startId;
            this.endId = endId;
        }

        @Override
        public void close() {
        }
    }

    static class NodeChunk extends EntityChunk {

        private final GraphStoreExport.NodeStore nodeStore;

        private final boolean hasLabels;
        private final boolean hasProperties;

        NodeChunk(GraphStoreExport.NodeStore nodeStore) {
            this.nodeStore = nodeStore;
            this.hasLabels = nodeStore.hasLabels();
            this.hasProperties = nodeStore.hasProperties();
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            if (id < endId) {
                visitor.id(id);

                if (hasLabels) {
                    String[] labels = nodeStore.labels(id);
                    visitor.labels(labels);

                    if (hasProperties) {
                        for (var label : labels) {
                            if (nodeStore.nodeProperties.containsKey(label)) {
                                for (var propertyKeyAndValue : nodeStore.nodeProperties.get(label).entrySet()) {
                                    visitor.property(
                                        propertyKeyAndValue.getKey(),
                                        propertyKeyAndValue.getValue().nodeProperty(id)
                                    );
                                }
                            }
                        }
                    }
                } else if (hasProperties) { // no label information, but node properties
                    for (var propertyKeyAndValue : nodeStore.nodeProperties.get(ALL_NODES.name).entrySet()) {
                        visitor.property(
                            propertyKeyAndValue.getKey(),
                            propertyKeyAndValue.getValue().nodeProperty(id)
                        );
                    }
                }

                visitor.endOfEntity();
                id++;
                return true;
            }
            return false;
        }
    }

    static class RelationshipChunk extends EntityChunk {

        private final Map<Pair<RelationshipType, Optional<String>>, RelationshipIterator> relationships;

        RelationshipChunk(Map<Pair<RelationshipType, Optional<String>>, RelationshipIterator> relationships) {
            this.relationships = relationships;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) {
            if (id < endId) {
                for (Map.Entry<Pair<RelationshipType, Optional<String>>, RelationshipIterator> relationship : relationships.entrySet()) {
                    RelationshipType relType = relationship.getKey().getOne();
                    Optional<String> relProperty = relationship.getKey().getTwo();
                    RelationshipIterator iterator = relationship.getValue();

                    iterator.forEachRelationship(id, Double.NaN, (s, t, propertyValue) -> {
                        visitor.startId(s);
                        visitor.endId(t);
                        visitor.type(relType.name);
                        relProperty.ifPresent(value -> visitor.property(value, propertyValue));
                        try {
                            visitor.endOfEntity();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        return true;
                    });
                }
                id++;
                return true;
            }
            return false;
        }
    }
}
