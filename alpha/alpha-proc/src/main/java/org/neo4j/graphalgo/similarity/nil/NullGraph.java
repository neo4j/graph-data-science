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
package org.neo4j.graphalgo.similarity.nil;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

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
    public long relationshipCount() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.relationshipCount is not implemented.");
    }

    @Override
    public boolean isUndirected() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.isUndirected is not implemented.");
    }

    @Override
    public boolean hasRelationshipProperty() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.hasRelationshipProperty is not implemented.");
    }

    @Override
    public void canRelease(boolean canRelease) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.canRelease is not implemented.");
    }

    @Override
    public RelationshipIntersect intersection(long maxDegree) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.intersection is not implemented.");
    }

    @Override
    public Graph concurrentCopy() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.concurrentCopy is not implemented.");
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.batchIterables is not implemented.");
    }

    @Override
    public int degree(long nodeId) {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.nil.NullGraph.degree is not implemented.");
    }

    @Override
    public NodeMapping nodeMapping() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.nodeMapping is not implemented.");
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.toMappedNodeId is not implemented.");
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.toOriginalNodeId is not implemented.");
    }

    @Override
    public boolean contains(long nodeId) {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.nil.NullGraph.contains is not implemented.");
    }

    @Override
    public long nodeCount() {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.nil.NullGraph.nodeCount is not implemented.");
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.forEachNode is not implemented.");
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.nodeIterator is not implemented.");
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.nodeLabelStream is not implemented.");
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.availableNodeLabels is not implemented.");
    }

    @Override
    public NodeProperties nodeProperties(String propertyKey) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.nodeProperties is not implemented.");
    }

    @Override
    public Set<String> availableNodeProperties() {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.availableNodeProperties is not implemented.");
    }

    @Override
    public long getTarget(long nodeId, long index) {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.nil.NullGraph.getTarget is not implemented.");
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.forEachRelationship is not implemented.");
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.forEachRelationship is not implemented.");
    }

    @Override
    public Stream<RelationshipCursor> streamRelationships(long nodeId, double fallbackValue) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.streamRelationships is not implemented.");
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.similarity.nil.NullGraph.exists is not implemented.");
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.relationshipProperty is not implemented.");
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        throw new UnsupportedOperationException(
            "org.neo4j.graphalgo.similarity.nil.NullGraph.relationshipProperty is not implemented.");
    }
}
