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
import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.BatchNodeIterable;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class SingleLabelInformation implements LabelInformation {

    private final long nodeCount;
    private final NodeLabel label;
    private final Set<NodeLabel> labelSet;

    private SingleLabelInformation(long nodeCount, NodeLabel label) {
        this.nodeCount = nodeCount;
        this.label = label;
        labelSet = Set.of(label);
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public void forEach(LabelInformationConsumer consumer) {
        throw new UnsupportedOperationException("There are not BitSets in empty label information");
    }

    @Override
    public LabelInformation filter(Collection<NodeLabel> nodeLabels) {
        return this;
    }

    @Override
    public BitSet unionBitSet(Collection<NodeLabel> nodeLabels, long nodeCount) {
       throw new UnsupportedOperationException("Union with empty label information is not supported");
    }

    @Override
    public long nodeCountForLabel(NodeLabel nodeLabel) {
        if (nodeLabel.equals(this.label)) {
            return this.nodeCount;
        }
        throw new IllegalArgumentException(formatWithLocale("No label information for label %s present", nodeLabel));
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel nodeLabel) {
        return nodeLabel.equals(label);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return labelSet;
    }

    @Override
    public List<NodeLabel> nodeLabelsForNodeId(long nodeId) {
        return List.of(label);
    }

    @Override
    public void forEachNodeLabel(long nodeId, IdMap.NodeLabelConsumer consumer) {
        consumer.accept(label);
    }

    @Override
    public void validateNodeLabelFilter(Collection<NodeLabel> nodeLabels) {
        List<ElementIdentifier> invalidLabels = nodeLabels
            .stream()
            .filter(filterLabel -> !filterLabel.equals(label))
            .collect(Collectors.toList());
        if (!invalidLabels.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Specified labels %s do not correspond to any of the node projections %s.",
                invalidLabels,
                availableNodeLabels()
            ));
        }
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator(
        Collection<NodeLabel> labels, long nodeCount
    ) {
        if (labels.size() == 1 && (labels.contains(label) || labels.contains(NodeLabel.ALL_NODES))) {
            return new BatchNodeIterable.IdIterator(nodeCount);
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Unknown labels: %s",
                StringJoining.join(labels.stream().map(NodeLabel::name))
            ));
        }
    }

    @Override
    public boolean isSingleLabel() {
        return true;
    }

    @Override
    public LabelInformation toMultiLabel(NodeLabel nodeLabelToMutate) {
        return LabelInformationBuilders
            .multiLabelWithCapacityAndLabelInformation(nodeCount, List.of(nodeLabelToMutate), availableNodeLabels())
            .build(nodeCount, LongUnaryOperator.identity());
    }

    static final class Builder implements LabelInformation.Builder {
        private final NodeLabel label;

        Builder(NodeLabel label) {this.label = label;}

        @Override
        public void addNodeIdToLabel(NodeLabel nodeLabel, long nodeId) {
            throw new UnsupportedOperationException("This builder does not support adding labels");
        }

        @Override
        public LabelInformation build(long nodeCount, LongUnaryOperator mappedIdFn) {
            return new SingleLabelInformation(nodeCount, label);
        }
    }
}
