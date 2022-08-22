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
import org.neo4j.gds.core.loading.FilteredIdMap;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;

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
    public long toMappedNodeId(long nodeId) {
        return idMap.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return idMap.toOriginalNodeId(nodeId);
    }

    @Override
    public long toRootNodeId(long nodeId) {
        return idMap.toRootNodeId(nodeId);
    }

    @Override
    public IdMap rootIdMap() {
        return idMap.rootIdMap();
    }

    @Override
    public boolean contains(long nodeId) {
        return idMap.contains(nodeId);
    }

    @Override
    public long nodeCount() {
        return idMap.nodeCount();
    }

    @Override
    public OptionalLong rootNodeCount() {
        return idMap.rootNodeCount();
    }

    @Override
    public long highestNeoId() {
        return idMap.highestNeoId();
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
    public List<NodeLabel> nodeLabels(long nodeId) {
        return idMap.nodeLabels(nodeId);
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        idMap.forEachNodeLabel(nodeId, consumer);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return idMap.availableNodeLabels();
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        return idMap.hasLabel(nodeId, label);
    }

    @Override
    public Optional<? extends FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        return idMap.withFilteredLabels(nodeLabels, concurrency);
    }
}
