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
import org.neo4j.gds.collections.primitive.PrimitiveLongIterable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongPredicate;

public abstract class IdMapAdapter implements IdMap {

    private final IdMap idMap;

    public IdMapAdapter(IdMap idMap) {
        this.idMap = idMap;
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(
        long batchSize
    ) {
        return idMap.batchIterables(batchSize);
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return idMap.toMappedNodeId(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return idMap.toOriginalNodeId(mappedNodeId);
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return idMap.toRootNodeId(mappedNodeId);
    }

    @Override
    public IdMap rootIdMap() {
        return idMap.rootIdMap();
    }

    @Override
    public boolean contains(long originalNodeId) {
        return idMap.contains(originalNodeId);
    }

    @Override
    public long nodeCount() {
        return idMap.nodeCount();
    }

    @Override
    public long nodeCount(NodeLabel nodeLabel) {
        return idMap.nodeCount(nodeLabel);
    }

    @Override
    public OptionalLong rootNodeCount() {
        return idMap.rootNodeCount();
    }

    @Override
    public long highestOriginalId() {
        return idMap.highestOriginalId();
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        idMap.forEachNode(consumer);
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator() {
        return idMap.nodeIterator();
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator(Set<NodeLabel> labels) {
        return idMap.nodeIterator(labels);
    }

    @Override
    public List<NodeLabel> nodeLabels(long mappedNodeId) {
        return idMap.nodeLabels(mappedNodeId);
    }

    @Override
    public void forEachNodeLabel(long mappedNodeId, NodeLabelConsumer consumer) {
        idMap.forEachNodeLabel(mappedNodeId, consumer);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return idMap.availableNodeLabels();
    }

    @Override
    public boolean hasLabel(long mappedNodeId, NodeLabel label) {
        return idMap.hasLabel(mappedNodeId, label);
    }

    public void addNodeLabel(NodeLabel nodeLabel) {
        idMap.addNodeLabel(nodeLabel);
    }

    public void addNodeIdToLabel(long nodeId, NodeLabel nodeLabel) {
        idMap.addNodeIdToLabel(nodeId, nodeLabel);
    }

    @Override
    public Optional<FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        return idMap.withFilteredLabels(nodeLabels, concurrency);
    }

}
