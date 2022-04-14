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
package org.neo4j.gds.api;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.loading.LabelInformation;
import org.neo4j.gds.core.utils.LazyBatchCollection;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.LongPredicate;

public abstract class LabeledIdMap implements IdMap {

    protected final LabelInformation labelInformation;
    private final long nodeCount;

    public LabeledIdMap(LabelInformation labelInformation, long nodeCount) {
        this.labelInformation = labelInformation;
        this.nodeCount = nodeCount;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        final long count = nodeCount();
        for (long i = 0L; i < count; i++) {
            if (!consumer.test(i)) {
                return;
            }
        }
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return new IdIterator(nodeCount());
    }

    @Override
    public PrimitiveLongIterator nodeIterator(Set<NodeLabel> labels) {
        return labelInformation.nodeIterator(labels, nodeCount());
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
    public Set<NodeLabel> availableNodeLabels() {
        return labelInformation.availableNodeLabels();
    }

    @Override
    public List<NodeLabel> nodeLabels(long nodeId) {
        return labelInformation.nodeLabelsForNodeId(nodeId);
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        labelInformation.forEachNodeLabel(nodeId, consumer);
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        return labelInformation.hasLabel(nodeId, label);
    }


}
