/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.FilterGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.loading.IdMap;

import java.util.Collection;
import java.util.function.LongPredicate;

public class NodeFilteredGraph extends FilterGraph {

    private final IdMap filteredIdMap;

    public NodeFilteredGraph(Graph graph, IdMap filteredIdMap) {
        super(graph);
        this.filteredIdMap = filteredIdMap;
    }

    @Override
    public RelationshipIntersect intersection() {
        return new FilteredGraphIntersectImpl(filteredIdMap, super.intersection());
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return filteredIdMap.nodeIterator();
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        return filteredIdMap.batchIterables(batchSize);
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        filteredIdMap.forEachNode(consumer);
    }

    @Override
    public int degree(long nodeId) {
        return super.degree(filteredIdMap.toOriginalNodeId(nodeId));
    }

    @Override
    public long nodeCount() {
        return filteredIdMap.nodeCount();
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return super.toMappedNodeId(filteredIdMap.toMappedNodeId(nodeId));
    }

    @Override
    public boolean contains(long nodeId) {
        return filteredIdMap.contains(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return super.toOriginalNodeId(filteredIdMap.toOriginalNodeId(nodeId));
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        super.forEachRelationship(filteredIdMap.toOriginalNodeId(nodeId), (s, t) -> filterAndConsume(s, t, consumer));
    }

    @Override
    public void forEachRelationship(
        long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer
    ) {
        super.forEachRelationship(filteredIdMap.toOriginalNodeId(nodeId), fallbackValue, (s, t, p) -> filterAndConsume(s, t, fallbackValue, consumer));
    }

    @Override
    public long getTarget(long sourceNodeId, long index) {
        HugeGraph.GetTargetConsumer consumer = new HugeGraph.GetTargetConsumer(index);
        forEachRelationship(filteredIdMap.toMappedNodeId(sourceNodeId), (s, t) -> filterAndConsume(s, t, consumer));
        return filteredIdMap.toOriginalNodeId(consumer.target);
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        return super.exists(filteredIdMap.toMappedNodeId(sourceNodeId), filteredIdMap.toMappedNodeId(targetNodeId));
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        return super.relationshipProperty(filteredIdMap.toMappedNodeId(sourceNodeId), filteredIdMap.toMappedNodeId(targetNodeId), fallbackValue);
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        return super.relationshipProperty(filteredIdMap.toMappedNodeId(sourceNodeId), filteredIdMap.toMappedNodeId(targetNodeId));
    }

    private boolean filterAndConsume(long source, long target, RelationshipConsumer consumer) {
        if (filteredIdMap.contains(source) && filteredIdMap.contains(target)) {
            long internalSourceId = filteredIdMap.toMappedNodeId(source);
            long internalTargetId = filteredIdMap.toMappedNodeId(target);
            return consumer.accept(internalSourceId, internalTargetId);
        }
        return false;
    }

    private boolean filterAndConsume(long source, long target, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        if (filteredIdMap.contains(source) && filteredIdMap.contains(target)) {
            long internalSourceId = filteredIdMap.toMappedNodeId(source);
            long internalTargetId = filteredIdMap.toMappedNodeId(target);
            return consumer.accept(internalSourceId, internalTargetId, fallbackValue);
        }
        return false;
    }
}
