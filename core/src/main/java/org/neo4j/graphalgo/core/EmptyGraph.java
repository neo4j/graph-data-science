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
package org.neo4j.graphalgo.core;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;

public class EmptyGraph implements Graph {

    @Override
    public long relationshipCount() {
        return 0;
    }

    @Override
    public void canRelease(final boolean canRelease) {

    }

    @Override
    public RelationshipIntersect intersection() {
        return null;
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(final int batchSize) {
        return null;
    }

    @Override
    public int degree(final long nodeId, final Direction direction) {
        return 0;
    }

    @Override
    public long toMappedNodeId(final long nodeId) {
        return 0;
    }

    @Override
    public long toOriginalNodeId(final long nodeId) {
        return 0;
    }

    @Override
    public boolean contains(final long nodeId) {
        return false;
    }

    @Override
    public long nodeCount() {
        return 0;
    }

    @Override
    public void forEachNode(final LongPredicate consumer) {

    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return new PrimitiveLongIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public long next() {
                return 0;
            }
        };
    }

    @Override
    public HugeWeightMapping nodeProperties(final String type) {
        return null;
    }

    @Override
    public Set<String> availableNodeProperties() {
        return null;
    }

    @Override
    public long getTarget(final long nodeId, final long index, final Direction direction) {
        return 0;
    }

    @Override
    public void forEachRelationship(final long nodeId, final Direction direction, final RelationshipConsumer consumer) {

    }

    @Override
    public void forEachRelationship(
            final long nodeId, final Direction direction, final WeightedRelationshipConsumer consumer) {

    }

    @Override
    public boolean exists(final long sourceNodeId, final long targetNodeId, final Direction direction) {
        return false;
    }

    @Override
    public double weightOf(final long sourceNodeId, final long targetNodeId) {
        return 0;
    }
}
