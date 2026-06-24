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
package org.neo4j.gds.core.huge;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.api.nodes.LabelInformation;
import org.neo4j.gds.api.nodes.NodeLabelConsumer;
import org.neo4j.gds.collections.primitive.PrimitiveLongCollections;
import org.neo4j.gds.collections.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.LazyBatchCollection;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongPredicate;

public class DirectIdMap implements IdMap {
    private final long nodeCount;

    public DirectIdMap(long nodeCount) {
        this.nodeCount = nodeCount;
    }

    @Override
    public String typeId() {
        return NO_TYPE;
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return originalNodeId;
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return mappedNodeId;
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return mappedNodeId;
    }

    @Override
    public long highestOriginalId() {
        return nodeCount;
    }

    @Override
    public boolean containsOriginalId(final long originalNodeId) {
        return originalNodeId < nodeCount;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public long nodeCount(NodeLabel nodeLabel) {
        throw new UnsupportedOperationException("No label information is present on DirectIdMap");
    }

    @Override
    public OptionalLong rootNodeCount() {
        return OptionalLong.of(nodeCount);
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(long batchSize) {
        return LazyBatchCollection.of(
            nodeCount(),
            batchSize,
            IdIterable::new
        );
    }

    @Override
    public List<NodeLabel> nodeLabels(long mappedNodeId) {
        return List.of();
    }

    @Override
    public void forEachNodeLabel(long mappedNodeId, NodeLabelConsumer consumer) {

    }

    @Override
    public LabelInformation labelInformation() {
        // DirectIdMap carries no label information; report an empty view that
        // agrees with availableNodeLabels()/nodeLabels()/hasLabel() below.
        return EmptyLabelInformation.INSTANCE;
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return Set.of();
    }

    @Override
    public boolean hasLabel(long mappedNodeId, NodeLabel label) {
        return false;
    }

    @Override
    public IdMap rootIdMap() {
        return this;
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        for (long i = 0; i < nodeCount; i++) {
            var shouldContinue = consumer.test(i);
            if (!shouldContinue) break;
        }
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator() {
        return PrimitiveLongCollections.range(0, nodeCount - 1);
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator(Set<NodeLabel> labels) {
        return nodeIterator();
    }

    public void addNodeLabel(NodeLabel nodeLabel) {
        throw new UnsupportedOperationException("Adding Node labels is not supported");
    }

    public void addNodeIdToLabel(long nodeId, NodeLabel nodeLabel) {
        throw new UnsupportedOperationException("Assigning Node labels to nodes is not supported");
    }

    /**
     * Label information for an id map that carries no labels. Read queries report
     * "no labels"; mutating or transforming the label information is unsupported.
     */
    private enum EmptyLabelInformation implements LabelInformation {
        INSTANCE;

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public void forEach(LabelInformationConsumer consumer) {
        }

        @Override
        public LabelInformation filter(Collection<NodeLabel> nodeLabels) {
            return this;
        }

        @Override
        public BitSet unionBitSet(Collection<NodeLabel> nodeLabels, long nodeCount) {
            return new BitSet(nodeCount);
        }

        @Override
        public BitSet bitSetForLabel(NodeLabel nodeLabel) {
            return new BitSet();
        }

        @Override
        public long nodeCountForLabel(NodeLabel nodeLabel) {
            return 0L;
        }

        @Override
        public boolean hasLabel(long nodeId, NodeLabel nodeLabel) {
            return false;
        }

        @Override
        public Set<NodeLabel> availableNodeLabels() {
            return Set.of();
        }

        @Override
        public List<NodeLabel> nodeLabelsForNodeId(long nodeId) {
            return List.of();
        }

        @Override
        public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        }

        @Override
        public void validateNodeLabelFilter(Collection<NodeLabel> nodeLabels) {
        }

        @Override
        public PrimitiveIterator.OfLong nodeIterator(Collection<NodeLabel> labels, long nodeCount) {
            return new PrimitiveIterator.OfLong() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public long nextLong() {
                    throw new NoSuchElementException();
                }
            };
        }

        @Override
        public void addLabel(NodeLabel nodeLabel) {
            throw new UnsupportedOperationException("Adding labels is not supported");
        }

        @Override
        public void addNodeIdToLabel(long nodeId, NodeLabel nodeLabel) {
            throw new UnsupportedOperationException("Adding node id to label is not supported");
        }

        @Override
        public boolean isSingleLabel() {
            return false;
        }

        @Override
        public LabelInformation toMultiLabel(NodeLabel nodeLabelToMutate) {
            throw new UnsupportedOperationException("Mutating empty label information is not supported");
        }
    }
}
