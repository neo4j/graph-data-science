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
package org.neo4j.graphalgo.centrality;

import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ParticleFilteringProc {
    @Context
    public KernelTransaction transaction;

    @Context
    public Log log;

    @Procedure(value = "gds.alpha.particleFiltering.stream", mode= Mode.READ)
    @Description("Returns nodes and an attached score given a nodeList, a score threshold, a #particles variable")
    public Stream<Output> particleFilteringStream(@Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration){
        CypherMapWrapper algoConfig = CypherMapWrapper.create(configuration);
        double numberParticles = algoConfig.getDouble("numberParticles", 10);
        double minThreshold = algoConfig.getDouble("minThreshold", 0.5);
        double dampingFactor = algoConfig.getDouble("dampingFactor", 0.85);
        boolean shuffleNeighbors = algoConfig.getBool("shuffleNeighbors", true);
        List<Node> sourceNodes = CypherMapWrapper.failOnNull("sourceNodes", algoConfig.getChecked("sourceNodes", Collections.emptyList(), List.class));

        String rawRelationshipType = algoConfig.getString("relationshipType", null);
        RelationshipType relationshipType = rawRelationshipType == null ? null : RelationshipType.withName(rawRelationshipType);
        Direction direction = Direction.valueOf(algoConfig.getString("direction", "OUTGOING"));

        double tao = 1.0 / numberParticles;

        Map<Long,Double> p= new HashMap<>();
        Map<Long,Double> v= new HashMap<>();
        int numberOfSourceNodes = sourceNodes.size();
        for (Node sourceNode : sourceNodes) {
            p.put(sourceNode.getId(), ((1.0 / numberOfSourceNodes) * (numberParticles)));
            v.put(sourceNode.getId(), ((1.0 / numberOfSourceNodes) * (numberParticles)));
        }

        Map<Long, Double> aux;
        double passing;

        while (!p.isEmpty()) {
            aux = new HashMap<>();
            for (Long node : p.keySet()) {
                double particles = p.get(node) * dampingFactor;
                Node startingNode = transaction.internalTransaction().getNodeById(node);
                List<Long> neighbours = new ArrayList<>();
                int neighboursCount = 0;

                for (Relationship relationship : getRelationships(startingNode, relationshipType, direction)) {
                    neighbours.add(relationship.getOtherNode(startingNode).getId());
                    neighboursCount++;
                }

                if(shuffleNeighbors) {
                    Collections.shuffle(neighbours);
                }

                for (Long neighbour : neighbours) {
                    if (particles <= tao)
                        break;
                    passing = Math.max(particles / neighboursCount, tao);
                    particles = particles - passing;
                    if (aux.containsKey(neighbour)) {
                        passing += aux.get(neighbour);
                    }
                    aux.put(neighbour, passing);
                }
            }
            p = aux;
            for (Long node : p.keySet()) {
                if (v.containsKey(node)) {
                    double value = v.get(node) + p.get(node);
                    v.put(node, value);
                } else
                    v.put(node, p.get(node));
            }
        }

        return v.entrySet().stream()
                .filter(entry -> entry.getValue() >= minThreshold)
                .map(entry -> new Output(entry.getKey(), entry.getValue()));
    }

    private Iterable<Relationship> getRelationships(
            Node node,
            RelationshipType relationshipType,
            Direction direction
    ) {
        return relationshipType == null ?
                node.getRelationships(direction) :
                node.getRelationships(direction, relationshipType);
    }

    protected class Output{
        public long nodeId;
        public double score;

        public Output(long nodeId, double score) {
            this.nodeId=nodeId;
            this.score=score;
        }
    }
}
