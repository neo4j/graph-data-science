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

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.RelationshipPredicate;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;

/**
 * Heavy weighted graph built of an adjacency matrix.
 *
 * @author mknblch
 */
public class HeavyGraph implements Graph, NodeProperties, RelationshipPredicate, RelationshipIntersect {

    public final static String TYPE = "heavy";

    private final IdMap nodeIdMap;
    private AdjacencyMatrix container;
    private WeightMapping relationshipWeights;

    private Map<String, WeightMapping> nodePropertiesMapping;

    private boolean canRelease = true;

    public HeavyGraph(
            IdMap nodeIdMap,
            AdjacencyMatrix container,
            final WeightMapping relationshipWeights,
            Map<String, WeightMapping> nodePropertiesMapping) {
        this.nodeIdMap = nodeIdMap;
        this.container = container;
        this.relationshipWeights = relationshipWeights;
        this.nodePropertiesMapping = nodePropertiesMapping;
    }

    @Override
    public long nodeCount() {
        return nodeIdMap.size();
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        nodeIdMap.forEach(consumer);
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return nodeIdMap.iterator();
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        return nodeIdMap.batchIterables(batchSize);
    }

    @Override
    public int degree(int nodeId, Direction direction) {
        return container.degree(nodeId, direction);
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer) {
        container.forEach(nodeId, direction, consumer);
    }

    @Override
    public void forEachRelationship(
            final int nodeId,
            final Direction direction,
            final WeightedRelationshipConsumer consumer) {
        container.forEach(nodeId, direction, relationshipWeights, consumer);
    }

    @Override
    public int toMappedNodeId(long originalNodeId) {
        return nodeIdMap.get(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(int mappedNodeId) {
        return nodeIdMap.toOriginalNodeId(mappedNodeId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return nodeIdMap.contains(nodeId);
    }

    @Override
    public double weightOf(final int sourceNodeId, final int targetNodeId) {
        return relationshipWeights.get(sourceNodeId, targetNodeId);
    }

    public boolean hasWeights() {
        return !(relationshipWeights instanceof NullWeightMap);
    }

    @Override
    public WeightMapping nodeProperties(String type) {
        return nodePropertiesMapping.get(type);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return nodePropertiesMapping.keySet();
    }

    @Override
    public void release() {
        if (!canRelease) return;
        container = null;
        relationshipWeights = null;
        nodePropertiesMapping.clear();
    }

    @Override
    public boolean exists(int sourceNodeId, int targetNodeId, Direction direction) {

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
    public int getTarget(int nodeId, int index, Direction direction) {
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
    public void intersectAll(long node, IntersectionConsumer consumer) {
        container.intersectAll(Math.toIntExact(node), consumer);
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
        return this;
    }
}
