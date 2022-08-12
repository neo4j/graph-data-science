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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

public abstract class GraphAdapter implements Graph {

    protected final Graph graph;

    public GraphAdapter(Graph graph) {
        this.graph = graph;
    }

    public Graph graph() {
        return graph;
    }

    @Override
    public long relationshipCount() {
        return graph.relationshipCount();
    }


    @Override
    public boolean hasRelationshipProperty() {
        return graph.hasRelationshipProperty();
    }

    @Override
    public void canRelease(boolean canRelease) {
        graph.canRelease(canRelease);
    }

    @Override
    public Optional<NodeFilteredGraph> asNodeFilteredGraph() {
        return graph.asNodeFilteredGraph();
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(long batchSize) {
        return graph.batchIterables(batchSize);
    }

    @Override
    public int degree(long nodeId) {
        return graph.degree(nodeId);
    }

    @Override
    public int degreeWithoutParallelRelationships(long nodeId) {
        return graph.degreeWithoutParallelRelationships(nodeId);
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return graph.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return graph.toOriginalNodeId(nodeId);
    }

    @Override
    public long toRootNodeId(long nodeId) {
        return graph.toRootNodeId(nodeId);
    }

    @Override
    public IdMap rootIdMap() {
        return graph.rootIdMap();
    }

    @Override
    public boolean contains(long nodeId) {
        return graph.contains(nodeId);
    }

    @Override
    public long nodeCount() {
        return graph.nodeCount();
    }

    @Override
    public OptionalLong rootNodeCount() {
        return graph.rootNodeCount();
    }

    @Override
    public long highestNeoId() {
        return graph.highestNeoId();
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        graph.forEachNode(consumer);
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator() {
        return graph.nodeIterator();
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator(Set<NodeLabel> labels) {
        return graph.nodeIterator(labels);
    }

    @Override
    public GraphSchema schema() {
        return graph.schema();
    }

    @Override
    public List<NodeLabel> nodeLabels(long nodeId) {
        return graph.nodeLabels(nodeId);
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        graph.forEachNodeLabel(nodeId, consumer);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return graph.availableNodeLabels();
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        return graph.hasLabel(nodeId, label);
    }

    @Override
    public NodePropertyValues nodeProperties(String propertyKey) {
        return graph.nodeProperties(propertyKey);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return graph.availableNodeProperties();
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        graph.forEachRelationship(nodeId, consumer);
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        graph.forEachRelationship(nodeId, fallbackValue, consumer);
    }

    @Override
    public Stream<RelationshipCursor> streamRelationships(long nodeId, double fallbackValue) {
        return graph.streamRelationships(nodeId, fallbackValue);
    }

    @Override
    public Graph relationshipTypeFilteredGraph(Set<RelationshipType> relationshipTypes) {
        return graph.relationshipTypeFilteredGraph(relationshipTypes);
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        return graph.exists(sourceNodeId, targetNodeId);
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        return graph.relationshipProperty(sourceNodeId, targetNodeId, fallbackValue);
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        return graph.relationshipProperty(sourceNodeId, targetNodeId);
    }

    @Override
    public void release() {
        graph.release();
    }

    @Override
    public void releaseTopology() {
        graph.releaseTopology();
    }

    @Override
    public void releaseProperties() {
        graph.releaseProperties();
    }

    @Override
    public boolean isMultiGraph() {
        // by filtering out elements the guarantee could become fulfilled, but we don't know
        // it would never go from fulfilled to not fulfilled however, so this is safe
        return graph.isMultiGraph();
    }
}
