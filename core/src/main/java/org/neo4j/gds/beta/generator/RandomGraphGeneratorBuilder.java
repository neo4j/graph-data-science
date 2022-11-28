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
package org.neo4j.gds.beta.generator;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.core.Aggregation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RandomGraphGeneratorBuilder {
    private long nodeCount;
    private long averageDegree;
    private RelationshipDistribution relationshipDistribution;
    private long seed = 0L;
    private Optional<NodeLabelProducer> maybeNodeLabelProducer = Optional.empty();
    private final Map<NodeLabel, Set<PropertyProducer<?>>> nodePropertyProducers = new HashMap<>();
    private Optional<PropertyProducer<double[]>> maybeRelationshipPropertyProducer = Optional.empty();
    private Aggregation aggregation = Aggregation.NONE;
    private Direction direction = Direction.DIRECTED;
    private RandomGraphGeneratorConfig.AllowSelfLoops allowSelfLoops = RandomGraphGeneratorConfig.AllowSelfLoops.NO;
    private RelationshipType relationshipType = RelationshipType.of("REL");

    public RandomGraphGeneratorBuilder nodeCount(long nodeCount) {
        this.nodeCount = nodeCount;
        return this;
    }

    public RandomGraphGeneratorBuilder averageDegree(long averageDegree) {
        this.averageDegree = averageDegree;
        return this;
    }

    public RandomGraphGeneratorBuilder relationshipType(RelationshipType relationshipType) {
        this.relationshipType = relationshipType;
        return this;
    }

    public RandomGraphGeneratorBuilder relationshipDistribution(RelationshipDistribution relationshipDistribution) {
        this.relationshipDistribution = relationshipDistribution;
        return this;
    }

    public RandomGraphGeneratorBuilder seed(long seed) {
        this.seed = seed;
        return this;
    }

    public RandomGraphGeneratorBuilder nodeLabelProducer(NodeLabelProducer nodeLabelProducer) {
        this.maybeNodeLabelProducer = Optional.of(nodeLabelProducer);
        return this;
    }

    public RandomGraphGeneratorBuilder nodePropertyProducer(PropertyProducer<?> nodePropertyProducer) {
        return addNodePropertyProducer(NodeLabel.ALL_NODES, nodePropertyProducer);
    }

    public RandomGraphGeneratorBuilder addNodePropertyProducer(NodeLabel nodeLabel, PropertyProducer<?> nodePropertyProducer) {
        // only add if the producer is not empty
        if (nodePropertyProducer.getPropertyName() != null) {
            this.nodePropertyProducers.computeIfAbsent(nodeLabel, ignore -> new HashSet<>()).add(nodePropertyProducer);
        }
        return this;
    }

    public RandomGraphGeneratorBuilder relationshipPropertyProducer(PropertyProducer<double[]> relationshipPropertyProducer) {
        this.maybeRelationshipPropertyProducer = Optional.of(relationshipPropertyProducer);
        return this;
    }

    public RandomGraphGeneratorBuilder aggregation(Aggregation aggregation) {
        this.aggregation = aggregation;
        return this;
    }

    public RandomGraphGeneratorBuilder direction(Direction direction) {
        this.direction = direction;
        return this;
    }

    public RandomGraphGeneratorBuilder allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops allowSelfLoops) {
        this.allowSelfLoops = allowSelfLoops;
        return this;
    }

    public RandomGraphGenerator build() {
        validate();
        return new RandomGraphGenerator(
            nodeCount,
            averageDegree,
            relationshipType,
            relationshipDistribution,
            seed,
            maybeNodeLabelProducer,
            nodePropertyProducers,
            maybeRelationshipPropertyProducer,
            aggregation,
            direction,
            allowSelfLoops
        );
    }

    private void validate() {
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("Must provide positive nodeCount");
        }
        if (averageDegree <= 0) {
            throw new IllegalArgumentException("Must provide positive averageDegree");
        }
        if (relationshipDistribution == null) {
            throw new IllegalArgumentException("Must provide a RelationshipDistribution");
        }
    }
}
