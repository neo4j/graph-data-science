/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.NodeOrRelationshipProperties;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;

abstract class SubGraph implements Graph {

    private final boolean hasRelationshipProperty;

    protected SubGraph(boolean hasRelationshipProperty) {this.hasRelationshipProperty = hasRelationshipProperty;}

    abstract int degree(long nodeId);

    @Override
    public final int degree(long nodeId, Direction direction) {
        return degree(nodeId);
    }

    abstract void forEach(long nodeId, RelationshipWithPropertyConsumer consumer);

    @Override
    public final void forEachRelationship(
            final long nodeId,
            final Direction direction,
            double fallbackValue,
            final RelationshipWithPropertyConsumer consumer) {
        forEach(nodeId, consumer);
    }

    @Override
    public final Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        return LazyBatchCollection.of(
                nodeCount(),
                batchSize,
                IdMap.IdIterable::new);
    }

    @Override
    public long relationshipCount() {
        return RELATIONSHIP_COUNT_NOT_SUPPORTED;
    }

    @Override
    public boolean hasRelationshipProperty() {
        return hasRelationshipProperty;
    }

    @Override
    public final void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        throw new UnsupportedOperationException("forEachRelationship is not supported.");
    }

    @Override
    public Direction getLoadDirection() {
        throw new UnsupportedOperationException("getLoadDirection is not supported.");
    }

    @Override
    public boolean isUndirected() {
        throw new UnsupportedOperationException("isUndirected is not supported.");
    }

    @Override
    public final double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        throw new UnsupportedOperationException("relationshipProperty is not supported.");
    }

    @Override
    public final boolean contains(long nodeId) {
        throw new UnsupportedOperationException("contains is not supported.");
    }

    @Override
    public final RelationshipIntersect intersection() {
        throw new UnsupportedOperationException("intersection is not supported.");
    }

    @Override
    public final long toMappedNodeId(long nodeId) {
        throw new UnsupportedOperationException("toMappedNodeId is not supported.");
    }

    @Override
    public final long toOriginalNodeId(long nodeId) {
        throw new UnsupportedOperationException("toOriginalNodeId is not supported.");
    }

    @Override
    public final void forEachNode(LongPredicate consumer) {
        throw new UnsupportedOperationException("forEachNode is not supported.");
    }

    @Override
    public final PrimitiveLongIterator nodeIterator() {
        throw new UnsupportedOperationException("nodeIterator is not supported.");
    }

    @Override
    public final NodeOrRelationshipProperties nodeProperties(String type) {
        throw new UnsupportedOperationException("nodeProperties is not supported.");
    }

    @Override
    public final long getTarget(long nodeId, long index, Direction direction) {
        throw new UnsupportedOperationException("getTarget is not supported.");
    }

    @Override
    public final boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        throw new UnsupportedOperationException("exists is not supported.");
    }

    @Override
    public final Set<String> availableNodeProperties() {
        throw new UnsupportedOperationException("availableNodeProperties is not supported.");
    }

    @Override
    public final String getType() {
        throw new UnsupportedOperationException("getType is not supported.");
    }

    @Override
    public final void canRelease(boolean canRelease) {
        throw new UnsupportedOperationException("canRelease is not supported.");
    }
}
