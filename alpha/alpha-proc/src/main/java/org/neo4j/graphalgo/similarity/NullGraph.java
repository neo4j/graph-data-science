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
package org.neo4j.graphalgo.similarity;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.loading.IdMap;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;

public class NullGraph implements Graph {

    // The NullGraph is used for non-product algos that don't use a graph.
    // It makes it a bit easier to adapt those algorithms to the new API,
    // as we can override graph creation and inject a NullGraph.
    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void releaseTopology() {}

    @Override
    public IdMap idMapping() {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.NullGraph.idMapping is not implemented.");
    }

    @Override
    public long relationshipCount() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.relationshipCount is not implemented.");
    }

    @Override
    public boolean isUndirected() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.isUndirected is not implemented.");
    }

    @Override
    public boolean hasRelationshipProperty() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.hasRelationshipProperty is not implemented.");
    }

    @Override
    public void canRelease(boolean canRelease) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.canRelease is not implemented.");
    }

    @Override
    public RelationshipIntersect intersection() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.intersection is not implemented.");
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.batchIterables is not implemented.");
    }

    @Override
    public int degree(long nodeId) {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.NullGraph.degree is not implemented.");
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.toMappedNodeId is not implemented.");
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.toOriginalNodeId is not implemented.");
    }

    @Override
    public boolean contains(long nodeId) {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.NullGraph.contains is not implemented.");
    }

    @Override
    public long nodeCount() {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.NullGraph.nodeCount is not implemented.");
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.forEachNode is not implemented.");
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.nodeIterator is not implemented.");
    }

    @Override
    public NodeProperties nodeProperties(String type) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.nodeProperties is not implemented.");
    }

    @Override
    public Set<String> availableNodeProperties() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.availableNodeProperties is not implemented.");
    }

    @Override
    public long getTarget(long nodeId, long index) {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.NullGraph.getTarget is not implemented.");
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.forEachRelationship is not implemented.");
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.forEachRelationship is not implemented.");
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.NullGraph.exists is not implemented.");
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.relationshipProperty is not implemented.");
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.NullGraph.relationshipProperty is not implemented.");
    }
}
