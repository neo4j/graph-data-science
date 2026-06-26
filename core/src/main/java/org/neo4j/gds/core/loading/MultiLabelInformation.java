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
import com.carrotsearch.hppc.BitSetIterator;
import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.BatchNodeIterable;
import org.neo4j.gds.api.NodeIdMapper;
import org.neo4j.gds.api.nodes.LabelInformation;
import org.neo4j.gds.api.nodes.NodeLabelConsumer;
import org.neo4j.gds.core.utils.paged.HugeAtomicGrowingBitSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class MultiLabelInformation implements LabelInformation {

    private final Map<NodeLabel, BitSet> labelInformation;

    private MultiLabelInformation(Map<NodeLabel, BitSet> labelInformation) {
        this.labelInformation = labelInformation;
    }

    @Override
    public boolean isEmpty() {
        return labelInformation.isEmpty();
    }

    @Override
    public void forEach(LabelInformationConsumer consumer) {
        for (Map.Entry<NodeLabel, BitSet> entry : labelInformation.entrySet()) {
            if (!consumer.accept(entry.getKey(), entry.getValue())) {
                return;
            }
        }
    }

    @Override
    public MultiLabelInformation filter(Collection<NodeLabel> nodeLabels, long filteredNodeCount, NodeIdMapper toFilteredNodeId) {
        return new MultiLabelInformation(nodeLabels
            .stream()
            .collect(Collectors.toMap(nodeLabel -> nodeLabel, nodeLabel -> {
                var rootBitSet = labelInformation.get(nodeLabel);
                var filteredBitSet = new BitSet(filteredNodeCount);
                var iterator = rootBitSet.iterator();
                for (long rootId = iterator.nextSetBit(); rootId != BitSetIterator.NO_MORE; rootId = iterator.nextSetBit()) {
                    filteredBitSet.set(toFilteredNodeId.map(rootId));
                }
                return filteredBitSet;
            })));
    }

    @Override
    public BitSet unionBitSet(Collection<NodeLabel> nodeLabels, long nodeCount) {
        assert labelInformation.keySet().containsAll(nodeLabels);

        BitSet unionBitSet = new BitSet(nodeCount);
        nodeLabels.forEach(label -> unionBitSet.union(labelInformation.get(label)));
        return unionBitSet;
    }

    @Override
    public BitSet bitSetForLabel(NodeLabel nodeLabel) {
        return labelInformation.get(nodeLabel);
    }

    @Override
    public long nodeCountForLabel(NodeLabel nodeLabel) {
        if (availableNodeLabels().contains(nodeLabel)) {
            return labelInformation.get(nodeLabel).cardinality();
        }
        throw new IllegalArgumentException(formatWithLocale("No label information for label %s present", nodeLabel));
    }

    @Override
    public void addLabel(NodeLabel nodeLabel) {
        labelInformation.computeIfAbsent(nodeLabel, (ignored) -> new BitSet());
    }

    @Override
    public void addNodeIdToLabel(long nodeId, NodeLabel nodeLabel) {
        labelInformation.get(nodeLabel).set(nodeId);
    }

    @Override
    public boolean isSingleLabel() {
        return false;
    }

    @Override
    public LabelInformation toMultiLabel(NodeLabel nodeLabelToMutate) {
        addLabel(nodeLabelToMutate);
        return this;
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel nodeLabel) {
        if (nodeLabel.equals(NodeLabel.ALL_NODES)) {
            return true;
        }
        var bitSet = labelInformation.get(nodeLabel);
        return bitSet != null && bitSet.get(nodeId);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return labelInformation.keySet();
    }

    @Override
    public List<NodeLabel> nodeLabelsForNodeId(long nodeId) {
        List<NodeLabel> labels = new ArrayList<>();
        forEach((nodeLabel, bitSet) -> {
            if (bitSet.get(nodeId)) {
                labels.add(nodeLabel);
            }
            return true;
        });
        return labels;
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        forEach((nodeLabel, bitSet) -> {
            if (bitSet.get(nodeId)) {
                return consumer.accept(nodeLabel);
            }
            return true;
        });
    }

    @Override
    public void validateNodeLabelFilter(Collection<NodeLabel> nodeLabels) {
        List<ElementIdentifier> invalidLabels = nodeLabels
            .stream()
            .filter(label -> !labelInformation.containsKey(label))
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
    public PrimitiveIterator.OfLong nodeIterator(Collection<NodeLabel> labels, long nodeCount) {
        if (labels.contains(NodeLabel.ALL_NODES)) {
            return new BatchNodeIterable.IdIterator(nodeCount);
        }
        return new BatchNodeIterable.BitSetIdIterator(unionBitSet(labels, nodeCount));
    }

    public static final class Builder implements LabelInformation.Builder {
        private final long expectedCapacity;
        private final Map<NodeLabel, HugeAtomicGrowingBitSet> labelInformation;
        private final Collection<NodeLabel> starNodeLabelMappings;

        private Builder(
            long expectedCapacity,
            Map<NodeLabel, HugeAtomicGrowingBitSet> labelInformation,
            Collection<NodeLabel> starNodeLabelMappings
        ) {
            this.expectedCapacity = expectedCapacity;
            this.labelInformation = labelInformation;
            this.starNodeLabelMappings = starNodeLabelMappings;
        }

        static Builder of(long expectedCapacity) {
            return of(expectedCapacity, List.of(), List.of());
        }

        static Builder of(
            long expectedCapacity,
            Collection<NodeLabel> availableNodeLabels,
            Collection<NodeLabel> starNodeLabelMappings
        ) {
            var nodeLabelBitSetMap = availableNodeLabels.stream().collect(
                Collectors.toConcurrentMap(
                    nodeLabel -> nodeLabel,
                    ignored -> HugeAtomicGrowingBitSet.create(expectedCapacity)
                )
            );
            return new Builder(expectedCapacity, nodeLabelBitSetMap, starNodeLabelMappings);
        }

        public void addNodeIdToLabel(NodeLabel nodeLabel, long nodeId) {
            labelInformation
                .computeIfAbsent(
                    nodeLabel,
                    (ignored) -> HugeAtomicGrowingBitSet.create(expectedCapacity)
                ).set(nodeId);
        }

        private Map<NodeLabel, BitSet> buildInner(long nodeCount, NodeIdMapper mappedIdFn) {
            // When the import bit sets are already keyed by the final mapped id (e.g. nodes loaded
            // through the internal id space), there is nothing to remap. We can hand the underlying
            // words straight to the hppc BitSet via a bulk copy instead of iterating and re-setting
            // every bit through mappedIdFn.
            boolean isIdentity = mappedIdFn == NodeIdMapper.IDENTITY;

            return this.labelInformation
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    var importBitSet = e.getValue();

                    if (isIdentity) {
                        long[] words = importBitSet.toLongArray(nodeCount);
                        return new BitSet(words, words.length);
                    }

                    var internBitSet = new BitSet(nodeCount);
                    importBitSet.forEachSetBit(neoId -> internBitSet.set(mappedIdFn.map(neoId)));

                    return internBitSet;
                }));
        }

        @Override
        public LabelInformation build(long nodeCount, NodeIdMapper mappedIdFn) {
            var labelInformation = buildInner(nodeCount, mappedIdFn);

            if (labelInformation.isEmpty() && starNodeLabelMappings.isEmpty()) {
                return LabelInformationBuilders.allNodes().build(nodeCount, mappedIdFn);
            }
            else if (labelInformation.size() == 1 && starNodeLabelMappings.isEmpty()) {
                return LabelInformationBuilders.singleLabel(labelInformation.keySet().iterator().next()).build(nodeCount, mappedIdFn);
            }

            // set the whole range for '*' projections
            for (NodeLabel starLabel : starNodeLabelMappings) {
                var bitSet = new BitSet(nodeCount);
                bitSet.set(0, nodeCount);
                labelInformation.put(starLabel, bitSet);
            }

            return new MultiLabelInformation(labelInformation);
        }
    }
}
