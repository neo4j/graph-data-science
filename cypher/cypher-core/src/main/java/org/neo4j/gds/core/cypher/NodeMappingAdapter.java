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
package org.neo4j.gds.core.cypher;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;

public class NodeMappingAdapter implements NodeMapping {

    private final NodeMapping nodeMapping;

    public NodeMappingAdapter(NodeMapping nodeMapping) {
        this.nodeMapping = nodeMapping;
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(
        long batchSize
    ) {
        return nodeMapping.batchIterables(batchSize);
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return nodeMapping.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return nodeMapping.toOriginalNodeId(nodeId);
    }

    @Override
    public long toRootNodeId(long nodeId) {
        return nodeMapping.toRootNodeId(nodeId);
    }

    @Override
    public boolean contains(long nodeId) {
        return nodeMapping.contains(nodeId);
    }

    @Override
    public long nodeCount() {
        return nodeMapping.nodeCount();
    }

    @Override
    public long rootNodeCount() {
        return nodeMapping.rootNodeCount();
    }

    @Override
    public long highestNeoId() {
        return nodeMapping.highestNeoId();
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        nodeMapping.forEachNode(consumer);
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return nodeMapping.nodeIterator();
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        return nodeMapping.nodeLabels(nodeId);
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        nodeMapping.forEachNodeLabel(nodeId, consumer);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return nodeMapping.availableNodeLabels();
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        return nodeMapping.hasLabel(nodeId, label);
    }
}
