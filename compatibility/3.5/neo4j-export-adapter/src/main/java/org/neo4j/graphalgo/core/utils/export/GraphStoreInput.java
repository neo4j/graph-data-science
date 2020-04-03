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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapGraph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.values.storable.Value;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class GraphStoreInput implements Input {

    private final GraphStore graphStore;

    private final int batchSize;

    GraphStoreInput(GraphStore graphStore, int batchSize) {
        this.graphStore = graphStore;
        this.batchSize = batchSize;
    }

    @Override
    public InputIterable nodes() {
        return () -> new NodeImporter(graphStore, batchSize);
    }

    @Override
    public InputIterable relationships() {
        return () -> new RelationshipImporter(graphStore, graphStore.nodeCount(), batchSize);
    }

    @Override
    public IdMapper idMapper(NumberArrayFactory numberArrayFactory) {
        return IdMappers.actual();
    }

    @Override
    public Collector badCollector() {
        return Collector.EMPTY;
    }

    @Override
    public Estimates calculateEstimates(ToIntFunction<Value[]> valueSizeCalculator) {
        long numberOfNodeProperties = graphStore.nodePropertyCount();
        long numberOfRelationshipProperties = graphStore.relationshipPropertyCount();

        return Inputs.knownEstimates(
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            numberOfNodeProperties,
            numberOfRelationshipProperties,
            numberOfNodeProperties * Double.BYTES,
            numberOfRelationshipProperties * Double.BYTES,
            0
        );
    }

    abstract static class GraphImporter implements InputIterator {

        private final int batchSize;
        private final long nodeCount;

        private long id;

        GraphImporter(long nodeCount, int batchSize) {
            this.batchSize = batchSize;
            this.nodeCount = nodeCount;
        }

        @Override
        public synchronized boolean next(InputChunk chunk) {
            if (id >= nodeCount) {
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

        private final GraphStore graphStore;

        NodeImporter(GraphStore graphStore, int batchSize) {
            super(graphStore.nodeCount(), batchSize);
            this.graphStore = graphStore;
        }

        @Override
        public InputChunk newChunk() {
            return new NodeChunk(graphStore);
        }
    }

    static class RelationshipImporter extends GraphImporter {
        private final Map<Pair<String, Optional<String>>, Graph> relationships;

        RelationshipImporter(GraphStore graphStore, long nodeCount, int batchSize) {
            super(nodeCount, batchSize);
            relationships = graphStore.relationshipTypes().stream().flatMap(relType -> {
                Set<String> relProperties = graphStore.relationshipPropertyKeys(relType);
                if (relProperties.isEmpty()) {
                    return Stream.of(Tuples.pair(relType, Optional.<String>empty()));
                } else {
                    return relProperties.stream().map(propertyKey -> Tuples.pair(relType, Optional.of(propertyKey)));
                }
            }).collect(Collectors.toMap(
                relTypeAndProperty -> relTypeAndProperty,
                relTypeAndProperty -> graphStore.getGraph(relTypeAndProperty.getOne(), relTypeAndProperty.getTwo())
            ));
        }

        @Override
        public InputChunk newChunk() {
            return new RelationshipChunk(relationships);
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

        private final GraphStore graphStore;

        NodeChunk(GraphStore graphStore) {
            this.graphStore = graphStore;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            if (id < endId) {
                visitor.id(id);

                graphStore.nodes().labels(id).forEach(label -> {
                    graphStore
                        .nodePropertyKeys(label).forEach(property -> {
                        NodeProperties nodeProperties = graphStore.nodeProperty(label, property);
                        visitor.property(property, nodeProperties.nodeProperty(id));
                    });
                });

                visitor.endOfEntity();
                id++;
                return true;
            }
            return false;
        }
    }

    static class RelationshipChunk extends EntityChunk {

        private final Map<Pair<String, Optional<String>>, Graph> relationships;

        RelationshipChunk(Map<Pair<String, Optional<String>>, Graph> relationships) {
            this.relationships = relationships;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) {
            if (id < endId) {
                for (Map.Entry<Pair<String, Optional<String>>, Graph> relationship : relationships.entrySet()) {
                    String relType = relationship.getKey().getOne();
                    Optional<String> relProperty = relationship.getKey().getTwo();
                    RelationshipIterator iterator = relationship.getValue();

                    iterator.forEachRelationship(id, Double.NaN, (s, t, propertyValue) -> {
                        visitor.startId(s);
                        visitor.endId(t);
                        visitor.type(relType);
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
