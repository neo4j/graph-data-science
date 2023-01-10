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
package org.neo4j.gds.core.loading;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.IdMap;

import java.util.Collection;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongUnaryOperator;

public interface LabelInformation {

    boolean isEmpty();

    void forEach(LabelInformationConsumer consumer);

    LabelInformation filter(Collection<NodeLabel> nodeLabels);

    BitSet unionBitSet(Collection<NodeLabel> nodeLabels, long nodeCount);

    long nodeCountForLabel(NodeLabel nodeLabel);

    boolean hasLabel(long nodeId, NodeLabel nodeLabel);

    Set<NodeLabel> availableNodeLabels();

    List<NodeLabel> nodeLabelsForNodeId(long nodeId);

    void forEachNodeLabel(long nodeId, IdMap.NodeLabelConsumer consumer);

    void validateNodeLabelFilter(Collection<NodeLabel> nodeLabels);

    PrimitiveIterator.OfLong nodeIterator(Collection<NodeLabel> labels, long nodeCount);

    default void addLabel(NodeLabel nodeLabel) {
        throw new UnsupportedOperationException("Adding labels is not supported");
    }

    default void addNodeIdToLabel(NodeLabel nodeLabel, long nodeId) {
        throw new UnsupportedOperationException("Adding node id to label is not supported");
    }

    boolean isSingleLabel();

    LabelInformation toMultiLabel(NodeLabel nodeLabelToMutate);

    interface LabelInformationConsumer {
        boolean accept(NodeLabel nodeLabel, BitSet bitSet);
    }

    interface Builder {
        void addNodeIdToLabel(NodeLabel nodeLabel, long nodeId);

        LabelInformation build(long nodeCount, LongUnaryOperator mappedIdFn);
    }
}
