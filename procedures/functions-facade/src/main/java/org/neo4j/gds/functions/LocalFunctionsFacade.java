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
package org.neo4j.gds.functions;

import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.legacycypherprojection.GraphProjectFromCypherConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.neo4j.gds.compat.GraphDatabaseApiProxy.getNodeById;

public class LocalFunctionsFacade implements FunctionsFacade {
    private final NeighbourFinder neighbourFinder = new NeighbourFinder();
    private final NodePropertyApplication nodePropertyApplication = new NodePropertyApplication();
    private final OneHotEncodingApplication oneHotEncodingApplication = new OneHotEncodingApplication();

    private final KernelTransaction kernelTransaction;
    private final RequestScopedDependencies requestScopedDependencies;

    public LocalFunctionsFacade(
        KernelTransaction kernelTransaction,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.kernelTransaction = kernelTransaction;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    /**
     * @see <a href="https://en.wikipedia.org/wiki/Adamic/Adar_index">Adamic–Adar index</a>
     */
    @Override
    public double adamicAdarIndex(Node node1, Node node2, Map<String, Object> configuration) {
        if (node1 == null || node2 == null) throw new RuntimeException("Nodes must not be null");

        var relationshipType = getRelationshipType(configuration);
        var direction = getDirection(configuration);

        var neighbors = neighbourFinder.findCommonNeighbours(node1, node2, relationshipType, direction);

        return neighbors.stream()
            .mapToDouble(nb -> 1.0 / Math.log(degree(nb, relationshipType, direction)))
            .sum();
    }

    @Override
    public Node asNode(Number nodeId) {
        return getNodeById(kernelTransaction, nodeId.longValue());
    }

    @Override
    public List<Node> asNodes(List<Number> nodeIds) {
        return nodeIds.stream()
            .map(nodeId -> getNodeById(kernelTransaction, nodeId.longValue()))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    @Override
    public double commonNeighbours(Node node1, Node node2, Map<String, Object> configuration) {
        if (node1 == null || node2 == null) throw new RuntimeException("Nodes must not be null");

        var relationshipType = getRelationshipType(configuration);
        var direction = getDirection(configuration);

        var neighbors = neighbourFinder.findCommonNeighbours(node1, node2, relationshipType, direction);

        return neighbors.size();
    }

    @Override
    public Object nodeProperty(String graphName, Object nodeId, String propertyKey, String nodeLabel) {
        return nodePropertyApplication.nodeProperty(
            requestScopedDependencies,
            graphName,
            nodeId,
            propertyKey,
            nodeLabel
        );
    }

    @Override
    public List<Long> oneHotEncoding(List<Object> availableValues, List<Object> selectedValues) {
        return oneHotEncodingApplication.encode(availableValues, selectedValues);
    }

    @Override
    public double preferentialAttachment(Node node1, Node node2, Map<String, Object> configuration) {
        if (node1 == null || node2 == null) throw new RuntimeException("Nodes must not be null");

        var relationshipType = getRelationshipType(configuration);
        var direction = getDirection(configuration);

        return degree(node1, relationshipType, direction) * degree(node2, relationshipType, direction);
    }

    /**
     * @see <a href="https://arxiv.org/pdf/0901.0553.pdf">Predicting Missing Links via Local Information</a>
     */
    @Override
    public double resourceAllocationSimilarity(Node node1, Node node2, Map<String, Object> configuration) {
        if (node1 == null || node2 == null) throw new RuntimeException("Nodes must not be null");

        var relationshipType = getRelationshipType(configuration);
        var direction = getDirection(configuration);

        var neighbors = neighbourFinder.findCommonNeighbours(node1, node2, relationshipType, direction);

        return neighbors.stream()
            .mapToDouble(nb -> 1.0 / degree(nb, relationshipType, direction))
            .sum();
    }

    @Override
    public double sameCommunity(Node node1, Node node2, String communityProperty) {
        if (!node1.hasProperty(communityProperty) || !node2.hasProperty(communityProperty)) return 0.0;

        if (node1.getProperty(communityProperty).equals(node2.getProperty(communityProperty))) return 1.0;

        return 0.0;
    }

    @Override
    public double totalNeighbours(Node node1, Node node2, Map<String, Object> configuration) {
        var relationshipType = getRelationshipType(configuration);
        var direction = getDirection(configuration);

        return neighbourFinder.findNeighbours(node1, node2, relationshipType, direction).size();
    }

    private int degree(Node node, RelationshipType relationshipType, Direction direction) {
        return relationshipType == null ? node.getDegree(direction) : node.getDegree(relationshipType, direction);
    }

    private Direction getDirection(Map<String, Object> config) {
        return Directions.fromString((String) config.getOrDefault(Constants.DIRECTION_KEY, Direction.BOTH.name()));
    }

    private RelationshipType getRelationshipType(Map<String, Object> config) {
        return config.getOrDefault(GraphProjectFromCypherConfig.RELATIONSHIP_QUERY_KEY, null) == null
            ? null
            : RelationshipType.withName((String) config.get(GraphProjectFromCypherConfig.RELATIONSHIP_QUERY_KEY));
    }
}
