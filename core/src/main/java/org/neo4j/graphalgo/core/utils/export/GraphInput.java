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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
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
import java.util.function.ToIntFunction;

public final class GraphInput implements Input {

    private final Graph graph;

    private final int batchSize;

    GraphInput(Graph graph, int batchSize) {
        this.graph = graph;
        this.batchSize = batchSize;
    }

    @Override
    public InputIterable nodes() {
        return () -> new NodeImporter(graph.nodeCount(), batchSize);
    }

    @Override
    public InputIterable relationships() {
        return () -> new RelationshipImporter(graph.concurrentCopy(), graph.nodeCount(), batchSize);
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
        return Inputs.knownEstimates(
            graph.nodeCount(),
            graph.relationshipCount(),
            graph.nodeCount(),
            graph.relationshipCount(),
            Long.BYTES,
            Long.BYTES,
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

        NodeImporter(long nodeCount, int batchSize) {
            super(nodeCount, batchSize);
        }

        @Override
        public InputChunk newChunk() {
            return new NodeChunk();
        }
    }

    static class RelationshipImporter extends GraphImporter {
        private final RelationshipIterator relationshipIterator;

        RelationshipImporter(RelationshipIterator relationshipIterator, long nodeCount, int batchSize) {
            super(nodeCount, batchSize);
            this.relationshipIterator = relationshipIterator;
        }

        @Override
        public InputChunk newChunk() {
            return new RelationshipChunk(relationshipIterator);
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
        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            if (id < endId) {
                visitor.id(id++);
                visitor.endOfEntity();
                return true;
            }
            return false;
        }
    }

    static class RelationshipChunk extends EntityChunk {

        private final RelationshipIterator relationshipIterator;

        RelationshipChunk(RelationshipIterator relationshipIterator) {
            this.relationshipIterator = relationshipIterator;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) {
            if (id < endId) {
                relationshipIterator.forEachRelationship(id, (s, t) -> {
                    visitor.startId(s);
                    visitor.endId(t);
                    visitor.type(0);
                    try {
                        visitor.endOfEntity();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return true;
                });
                id++;
                return true;
            }
            return false;
        }
    }
}