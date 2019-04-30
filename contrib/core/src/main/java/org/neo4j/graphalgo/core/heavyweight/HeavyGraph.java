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
package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.LongPredicate;

/**
 * Heavy weighted graph built of an adjacency matrix.
 *
 * @author mknblch
 */
public class HeavyGraph implements Graph {

    public final static String TYPE = "heavy";

    private final IntIdMap nodeIdMap;
    private AdjacencyMatrix container;

    private Map<String, WeightMapping> nodePropertiesMapping;

    private boolean canRelease = true;

    public HeavyGraph(
            IntIdMap nodeIdMap,
            AdjacencyMatrix container,
            Map<String, WeightMapping> nodePropertiesMapping) {
        this.nodeIdMap = nodeIdMap;
        this.container = container;
        this.nodePropertiesMapping = nodePropertiesMapping;
    }

    @Override
    public long nodeCount() {
        return nodeIdMap.size();
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        nodeIdMap.forEachNode(consumer);
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return nodeIdMap.iterator();
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        return nodeIdMap.longBatchIterables(batchSize);
    }

    @Override
    public int degree(long nodeId, Direction direction) {
        return container.degree(nodeId, direction);
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        container.forEach(nodeId, direction, consumer);
    }

    @Override
    public void forEachRelationship(
            final long nodeId,
            final Direction direction,
            final WeightedRelationshipConsumer consumer) {
        container.forEach(nodeId, direction, consumer);
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return nodeIdMap.get(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return nodeIdMap.toOriginalNodeId(mappedNodeId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return nodeIdMap.contains(nodeId);
    }

    @Override
    public double weightOf(final long sourceNodeId, final long targetNodeId) {
        checkSize(sourceNodeId, targetNodeId);
        return container.weightOf((int) sourceNodeId, (int) targetNodeId);
    }

    public boolean hasWeights() {
        return container.hasWeights();
    }

    @Override
    public HugeWeightMapping nodeProperties(String type) {
        WeightMapping weightMapping = nodePropertiesMapping.get(type);
        return new HugeWeightMapping() {

            @Override
            public double weight(long source, long target) {
                checkSize(source, target);
                return weightMapping.get((int) source, (int) target);
            }

            @Override
            public double weight(long source, long target, double defaultValue) {
                checkSize(source, target);
                return weightMapping.get(RawValues.combineIntInt((int) source, (int) target), defaultValue);
            }

            @Override
            public long release() {
                return 0;
            }
        };
    }

    @Override
    public Set<String> availableNodeProperties() {
        return nodePropertiesMapping.keySet();
    }

    @Override
    public void release() {
        if (!canRelease) return;
        container = null;
        nodePropertiesMapping.clear();
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {

        switch (direction) {
            case OUTGOING:
                return container.hasOutgoing(sourceNodeId, targetNodeId);

            case INCOMING:
                return container.hasIncoming(sourceNodeId, targetNodeId);

            default:
                return container.hasOutgoing(sourceNodeId, targetNodeId) || container.hasIncoming(
                        sourceNodeId,
                        targetNodeId);
        }
    }

    @Override
    public long getTarget(long nodeId, long index, Direction direction) {
        switch (direction) {
            case OUTGOING:
                return container.getTargetOutgoing(nodeId, index);

            case INCOMING:
                return container.getTargetIncoming(nodeId, index);

            default:
                return container.getTargetBoth(nodeId, index);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void canRelease(boolean canRelease) {
        this.canRelease = canRelease;
    }

    @Override
    public RelationshipIntersect intersection() {
        return (nodeId, consumer) -> container.intersectAll(Math.toIntExact(nodeId), consumer);
    }

    //TODO: Remove this once we have confidence
    // Could turn into assertion and go with compiling with/without -ea
    public static void checkSize(long... values) {
        for (long v : values) {
            if (v > Integer.MAX_VALUE) {
                throw new IllegalStateException("Long value too large for int: " + v);
            }
        }
    }
}
