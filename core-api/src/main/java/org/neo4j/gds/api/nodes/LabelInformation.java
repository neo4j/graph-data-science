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
package org.neo4j.gds.api.nodes;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.NodeIdMapper;

import java.util.Collection;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongUnaryOperator;

public interface LabelInformation {

    boolean isEmpty();

    void forEach(LabelInformationConsumer consumer);

    /**
     * Restricts this label information to the given {@code nodeLabels} and re-indexes the
     * retained node ids into a compact filtered id space via {@code toFilteredNodeId}. The
     * returned label information is keyed by filtered node ids in {@code [0, filteredNodeCount)}.
     */
    LabelInformation filter(Collection<NodeLabel> nodeLabels, long filteredNodeCount, LongUnaryOperator toFilteredNodeId);

    BitSet unionBitSet(Collection<NodeLabel> nodeLabels, long nodeCount);

    BitSet bitSetForLabel(NodeLabel nodeLabel);

    long nodeCountForLabel(NodeLabel nodeLabel);

    boolean hasLabel(long nodeId, NodeLabel nodeLabel);

    Set<NodeLabel> availableNodeLabels();

    List<NodeLabel> nodeLabelsForNodeId(long nodeId);

    void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer);

    void validateNodeLabelFilter(Collection<NodeLabel> nodeLabels);

    PrimitiveIterator.OfLong nodeIterator(Collection<NodeLabel> labels, long nodeCount);

    void addLabel(NodeLabel nodeLabel);

    void addNodeIdToLabel(long nodeId, NodeLabel nodeLabel);

    boolean isSingleLabel();

    LabelInformation toMultiLabel(NodeLabel nodeLabelToMutate);

    interface LabelInformationConsumer {
        boolean accept(NodeLabel nodeLabel, BitSet bitSet);
    }

    interface Builder {
        void addNodeIdToLabel(NodeLabel nodeLabel, long nodeId);

        LabelInformation build(long nodeCount, NodeIdMapper mappedIdFn);
    }
}
