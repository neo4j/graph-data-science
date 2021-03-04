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
package org.neo4j.graphalgo.similarity.nil;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

/**
 * The NullGraph is used for non-product algos that don't use a graph.
 * It makes it a bit easier to adapt those algorithms to the new API,
 * as we can override graph creation and inject a NullGraph.
 */
public class NullGraph implements Graph {

    /*
     * The NullGraph doesn't have any nodes or rels, but it isn't empty because then the algo will not be run.
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void releaseTopology() {}

    @Override
    public long relationshipCount() {
        return 0L;
    }

    @Override
    public boolean isUndirected() {
        return false;
    }

    @Override
    public boolean isMultiGraph() {
        return false;
    }

    @Override
    public boolean hasRelationshipProperty() {
        return false;
    }

    @Override
    public void canRelease(boolean canRelease) {}

    @Override
    public Graph concurrentCopy() {
        return this;
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        return Set.of();
    }

    @Override
    public int degree(long nodeId) {
        return 0;
    }

    @Override
    public int degreeWithoutParallelRelationships(long nodeId) {
        return 0;
    }

    @Override
    public NodeMapping nodeMapping() {
        throw new NullGraphStore.NullGraphException();
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        throw new NullGraphStore.NullGraphException();
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        throw new NullGraphStore.NullGraphException();
    }

    @Override
    public long toRootNodeId(long nodeId) {
        throw new NullGraphStore.NullGraphException();
    }

    @Override
    public boolean contains(long nodeId) {
        return false;
    }

    @Override
    public long nodeCount() {
        return 0L;
    }

    @Override
    public long rootNodeCount() {
        return 0L;
    }

    @Override
    public void forEachNode(LongPredicate consumer) {}

    @Override
    public PrimitiveLongIterator nodeIterator() {
        throw new NullGraphStore.NullGraphException();
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        return Set.of();
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return Set.of();
    }

    @Override
    public GraphSchema schema() {
        return GraphSchema.of(NodeSchema.builder().build(), RelationshipSchema.builder().build());
    }

    @Override
    public NodeProperties nodeProperties(String propertyKey) {
        throw new NullGraphStore.NullGraphException();
    }

    @Override
    public Set<String> availableNodeProperties() {
        return Set.of();
    }

    @Override
    public long getTarget(long nodeId, long index) {
        throw new NullGraphStore.NullGraphException();
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {}

    @Override
    public void forEachRelationship(
        long nodeId,
        double fallbackValue,
        RelationshipWithPropertyConsumer consumer
    ) {}

    @Override
    public Stream<RelationshipCursor> streamRelationships(long nodeId, double fallbackValue) {
        return Stream.empty();
    }

    @Override
    public RelationshipIterator typeFilteredIterator(Set<RelationshipType> relationshipTypes) {
        return this;
    }

    @Override
    public Set<RelationshipType> relationshipTypes(long source, long target) {
        throw new NullGraphStore.NullGraphException();
    }

    @Override
    public Set<RelationshipType> availableRelationshipTypes() {
        return Set.of();
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        return false;
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        throw new NullGraphStore.NullGraphException();
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        throw new NullGraphStore.NullGraphException();
    }
}
