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
import com.carrotsearch.hppc.IntObjectMap;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;

public final class LabelInformation {

    public static LabelInformation from(Map<NodeLabel, BitSet> labelInformation) {
        return new LabelInformation(labelInformation);
    }

    private final Map<NodeLabel, BitSet> labelInformation;

    private LabelInformation(Map<NodeLabel, BitSet> labelInformation) {
        this.labelInformation = labelInformation;
    }

    public boolean isEmpty() {
        return labelInformation.isEmpty();
    }

    public Set<NodeLabel> labelSet() {
        return labelInformation.keySet();
    }

    public void forEach(LabelInformationConsumer consumer) {
        for (Map.Entry<NodeLabel, BitSet> entry : labelInformation.entrySet()) {
            if (!consumer.accept(entry.getKey(), entry.getValue())) {
                return;
            }
        }
    }

    public LabelInformation filter(Collection<NodeLabel> nodeLabels) {
        return new LabelInformation(nodeLabels
            .stream()
            .collect(Collectors.toMap(nodeLabel -> nodeLabel, labelInformation::get)));
    }

    public BitSet unionBitSet(Collection<NodeLabel> nodeLabels, long nodeCount) {
        BitSet unionBitSet = new BitSet(nodeCount);
        nodeLabels.forEach(label -> unionBitSet.union(labelInformation.get(label)));
        return unionBitSet;
    }

    public boolean hasLabel(long nodeId, NodeLabel nodeLabel) {
        var bitSet = labelInformation.get(nodeLabel);
        return bitSet != null && bitSet.get(nodeId);
    }

    public interface LabelInformationConsumer {
        boolean accept(NodeLabel nodeLabel, BitSet bitSet);
    }

    public static Builder emptyBuilder(AllocationTracker tracker) {
        return new Builder(tracker);
    }

    public static Builder builder(long nodeCount, IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping, AllocationTracker tracker) {
        var nodeLabelBitSetMap = StreamSupport.stream(
            labelTokenNodeLabelMapping.values().spliterator(),
            false
        )
            .flatMap(cursor -> cursor.value.stream())
            .distinct()
            .collect(Collectors.toMap(
                nodeLabel -> nodeLabel,
                nodeLabel -> HugeAtomicBitSet.create(nodeCount, tracker))
            );

        // set the whole range for '*' projections
        for (NodeLabel starLabel : labelTokenNodeLabelMapping.getOrDefault(ANY_LABEL, Collections.emptyList())) {
            nodeLabelBitSetMap.get(starLabel).set(0, nodeCount);
        }

        return new Builder(nodeLabelBitSetMap, tracker);
    }

    public static class Builder {
        private final AllocationTracker tracker;

        final Map<NodeLabel, HugeAtomicBitSet> labelInformation;

        Builder(AllocationTracker tracker) {
            this(new ConcurrentHashMap<>(), tracker);
        }

        Builder(Map<NodeLabel, HugeAtomicBitSet> labelInformation, AllocationTracker tracker) {
            this.labelInformation = labelInformation;
            this.tracker = tracker;
        }

        public void addNodeIdToLabel(NodeLabel nodeLabel, long nodeId, long nodeCount) {
            labelInformation
                .computeIfAbsent(
                    nodeLabel,
                    (ignored) -> HugeAtomicBitSet.create(nodeCount, tracker)
                )
                .set(nodeId);
        }

        public LabelInformation build() {
            return new LabelInformation(labelInformation
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toBitSet())));
        }
    }
}
