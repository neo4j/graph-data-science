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
package org.neo4j.graphalgo.beta.generator;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig.AllowSelfLoops;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.builder.GraphBuilder;
import org.neo4j.graphalgo.core.loading.builder.NodesBuilder;
import org.neo4j.graphalgo.core.loading.builder.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.LongUnaryOperator;

public final class RandomGraphGenerator {

    private final AllocationTracker allocationTracker;
    private final long nodeCount;
    private final long averageDegree;
    private final Random random;
    private final RelationshipDistribution relationshipDistribution;
    private final Aggregation aggregation;
    private final Orientation orientation;
    private final AllowSelfLoops allowSelfLoops;
    private final Optional<PropertyProducer> maybeRelationshipPropertyProducer;
    private final Optional<PropertyProducer> maybeNodePropertyProducer;

    public RandomGraphGenerator(
        long nodeCount,
        long averageDegree,
        RelationshipDistribution relationshipDistribution,
        @Nullable Long seed,
        Optional<PropertyProducer> maybeNodePropertyProducer,
        Optional<PropertyProducer> maybeRelationshipPropertyProducer,
        Aggregation aggregation,
        Orientation orientation,
        AllowSelfLoops allowSelfLoops,
        AllocationTracker allocationTracker
    ) {
        this.relationshipDistribution = relationshipDistribution;
        this.maybeNodePropertyProducer = maybeNodePropertyProducer;
        this.maybeRelationshipPropertyProducer = maybeRelationshipPropertyProducer;
        this.allocationTracker = allocationTracker;
        this.nodeCount = nodeCount;
        this.averageDegree = averageDegree;
        this.aggregation = aggregation;
        this.orientation = orientation;
        this.allowSelfLoops = allowSelfLoops;
        this.random = new Random();
        if (seed != null) {
            this.random.setSeed(seed);
        } else {
            this.random.setSeed(1);
        }
    }

    public static RandomGraphGeneratorBuilder builder() {
        return new RandomGraphGeneratorBuilder();
    }

    public HugeGraph generate() {
        var idMapBuilder = GraphBuilder.idMapBuilder(
            nodeCount,
            false,
            1,
            allocationTracker
        );

        generateNodes(idMapBuilder);

        IdMap idMap = idMapBuilder.build();
        RelationshipsBuilder relationshipsBuilder = GraphBuilder.createRelImporter(
            idMap,
            orientation,
            maybeRelationshipPropertyProducer.isPresent(),
            aggregation,
            false,
            Pools.DEFAULT,
            allocationTracker
        );

        generateRelationships(relationshipsBuilder);
        if (maybeNodePropertyProducer.isPresent()) {
            NodeProperties nodeProperties = generateNodeProperties();
            String propertyName = maybeNodePropertyProducer.get().getPropertyName();
            return GraphBuilder.create(
                idMap,
                Map.of(propertyName, nodeProperties),
                relationshipsBuilder.build(),
                allocationTracker
            );
        } else {
            return GraphBuilder.create(
                idMap,
                relationshipsBuilder.build(),
                allocationTracker
            );
        }
    }

    private NodeProperties generateNodeProperties() {
        PropertyProducer nodePropertyProducer = maybeNodePropertyProducer.get();
        HugeDoubleArray nodePropertiesArray = HugeDoubleArray.newArray(nodeCount, allocationTracker);

        for (long i = 0; i < nodeCount; i++) {
            nodePropertiesArray.set(i, nodePropertyProducer.getPropertyValue(random));
        }

        return nodePropertiesArray.asNodeProperties();
    }

    public RelationshipDistribution getRelationshipDistribution() {
        return relationshipDistribution;
    }

    public Optional<PropertyProducer> getMaybeRelationshipPropertyProducer() {
        return maybeRelationshipPropertyProducer;
    }

    private void generateNodes(NodesBuilder nodesBuilder) {
        for (long i = 0; i < nodeCount; i++) {
            nodesBuilder.addNode(i);
        }
    }

    private void generateRelationships(RelationshipsBuilder relationshipsImporter) {
        LongUnaryOperator degreeProducer = relationshipDistribution.degreeProducer(nodeCount, averageDegree, random);
        LongUnaryOperator relationshipProducer = relationshipDistribution.relationshipProducer(
            nodeCount,
            averageDegree,
            random
        );
        PropertyProducer relationshipPropertyProducer = maybeRelationshipPropertyProducer.orElse(new PropertyProducer.EmptyPropertyProducer());

        long degree, targetId;
        double property;

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            degree = degreeProducer.applyAsLong(nodeId);
            
            for (int j = 0; j < degree; j++) {
                if (allowSelfLoops.value()) {
                    targetId = relationshipProducer.applyAsLong(nodeId);
                } else {
                    while ((targetId = relationshipProducer.applyAsLong(nodeId)) == nodeId) {}
                }
                assert (targetId < nodeCount);
                property = relationshipPropertyProducer.getPropertyValue(random);
                relationshipsImporter.addFromInternal(nodeId, targetId, property);
            }
        }
    }
}
