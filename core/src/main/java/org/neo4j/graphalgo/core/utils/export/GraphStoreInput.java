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
package org.neo4j.graphalgo.core.utils.export;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.compat.CompatInput;
import org.neo4j.graphalgo.compat.CompatPropertySizeCalculator;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.ReadableGroups;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.neo4j.graphalgo.NodeLabel.ALL_NODES;

public final class GraphStoreInput implements CompatInput {

    private final NodeStore nodeStore;

    private final RelationshipStore relationshipStore;

    private final int batchSize;

    GraphStoreInput(
        NodeStore nodeStore,
        RelationshipStore relationshipStore,
        int batchSize
    ) {
        this.nodeStore = nodeStore;
        this.relationshipStore = relationshipStore;
        this.batchSize = batchSize;
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        return () -> new NodeImporter(nodeStore, batchSize);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return () -> new RelationshipImporter(relationshipStore, batchSize);
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

        private final NodeStore nodeStore;

        NodeImporter(NodeStore nodeStore, int batchSize) {
            super(nodeStore.nodeCount, batchSize);
            this.nodeStore = nodeStore;
        }

        @Override
        public InputChunk newChunk() {
            return new NodeChunk(nodeStore);
        }
    }

    static class RelationshipImporter extends GraphImporter {

        private final RelationshipStore relationshipStore;

        RelationshipImporter(RelationshipStore relationshipStore, int batchSize) {
            super(relationshipStore.nodeCount, batchSize);
            this.relationshipStore = relationshipStore;
        }

        @Override
        public InputChunk newChunk() {
            return new RelationshipChunk(relationshipStore.concurrentCopy());
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

        private final NodeStore nodeStore;

        private final boolean hasLabels;
        private final boolean hasProperties;

        NodeChunk(NodeStore nodeStore) {
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
                                    exportProperty(visitor, propertyKeyAndValue);
                                }
                            }
                        }
                    }
                } else if (hasProperties) { // no label information, but node properties
                    for (var propertyKeyAndValue : nodeStore.nodeProperties.get(ALL_NODES.name).entrySet()) {
                        exportProperty(visitor, propertyKeyAndValue);
                    }
                }

                visitor.endOfEntity();
                id++;
                return true;
            }
            return false;
        }

        private void exportProperty(InputEntityVisitor visitor, Map.Entry<String, NodeProperties> propertyKeyAndValue) {
            var value = propertyKeyAndValue.getValue().getObject(id);
            if (value != null) {
                visitor.property(propertyKeyAndValue.getKey(), value);
            }
        }
    }

    static class RelationshipChunk extends EntityChunk {

        private final RelationshipStore relationshipStore;

        RelationshipChunk(RelationshipStore relationshipStore) {
            this.relationshipStore = relationshipStore;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) {
            if (id < endId) {
                for (var entry : relationshipStore.relationshipIterators.entrySet()) {
                    var relationshipType = entry.getKey();
                    var relationshipIterator = entry.getValue();
                    try {
                        relationshipIterator.forEachRelationship(id, relationshipType.name, visitor);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                id++;
                return true;
            }
            return false;
        }
    }
}
