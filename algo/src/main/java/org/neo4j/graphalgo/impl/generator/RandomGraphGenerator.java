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

package org.neo4j.graphalgo.impl.generator;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.GraphGenerator;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.Optional;
import java.util.Random;
import java.util.function.LongUnaryOperator;

public final class RandomGraphGenerator {

    private final AllocationTracker allocationTracker;
    private final long nodeCount;
    private final long averageDegree;
    private final Random random;
    private final RelationshipDistribution relationshipDistribution;
    private final Optional<RelationshipPropertyProducer> maybePropertyProducer;

    public static Graph generate(int nodeCount, int averageDegree) {
        return generate(nodeCount, averageDegree, RelationshipDistribution.POWER_LAW);
    }

    public static Graph generate(int nodeCount, int averageDegree, RelationshipDistribution distribution) {
        return generate(nodeCount, averageDegree, distribution, null);
    }

    public static Graph generate(int nodeCount, int averageDegree, RelationshipDistribution distribution, @Nullable Long seed) {
        return new RandomGraphGenerator(
            nodeCount,
            averageDegree,
            distribution,
            seed,
            Optional.empty(),
            AllocationTracker.EMPTY
        ).generate();
    }

    public RandomGraphGenerator(
        long nodeCount,
        long averageDegree,
        RelationshipDistribution relationshipDistribution,
        @Nullable Long seed,
        Optional<RelationshipPropertyProducer> maybePropertyProducer,
        AllocationTracker allocationTracker
    ) {
        this.relationshipDistribution = relationshipDistribution;
        this.maybePropertyProducer = maybePropertyProducer;
        this.allocationTracker = allocationTracker;
        this.nodeCount = nodeCount;
        this.averageDegree = averageDegree;
        this.random = new Random();
        if (seed != null) {
            this.random.setSeed(seed);
        }
    }

    public RandomGraphGenerator(
        long nodeCount,
        long averageDegree,
        RelationshipDistribution relationshipDistribution,
        Optional<RelationshipPropertyProducer> maybePropertyProducer,
        AllocationTracker allocationTracker
    ) {
        this(nodeCount, averageDegree, relationshipDistribution, null, maybePropertyProducer, allocationTracker);
    }

    public HugeGraph generate() {
        GraphGenerator.NodeImporter nodeImporter = GraphGenerator.createNodeImporter(
            nodeCount,
            Pools.DEFAULT,
            allocationTracker
        );

        generateNodes(nodeImporter);

        GraphGenerator.RelImporter relationshipsImporter = GraphGenerator.createRelImporter(
            nodeImporter,
            Direction.BOTH,
            false,
            maybePropertyProducer.isPresent(),
            DeduplicationStrategy.NONE
        );

        generateRelationships(relationshipsImporter);
        return relationshipsImporter.buildGraph();
    }

    long getNodeCount() {
        return nodeCount;
    }

    long getAverageDegree() {
        return averageDegree;
    }

    public RelationshipDistribution getRelationshipDistribution() {
        return relationshipDistribution;
    }

    public Optional<RelationshipPropertyProducer> getMaybePropertyProducer() {
        return maybePropertyProducer;
    }

    public boolean shouldGenerateRelationshipProperty() {
        return maybePropertyProducer.isPresent();
    }

    private void generateNodes(GraphGenerator.NodeImporter nodeImporter) {
        for (long i = 0; i < nodeCount; i++) {
            nodeImporter.addNode(i);
        }
    }

    private void generateRelationships(GraphGenerator.RelImporter relationshipsImporter) {
        LongUnaryOperator degreeProducer = relationshipDistribution.degreeProducer(nodeCount, averageDegree, random);
        LongUnaryOperator relationshipProducer = relationshipDistribution.relationshipProducer(
            nodeCount,
            averageDegree,
            random
        );
        RelationshipPropertyProducer relationshipPropertyProducer = maybePropertyProducer.orElse(new EmptyRelationshipPropertyProducer());

        long degree, targetId;
        double property;

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            degree = degreeProducer.applyAsLong(nodeId);

            for (int j = 0; j < degree; j++) {
                targetId = relationshipProducer.applyAsLong(nodeId);
                assert (targetId < nodeCount);
                property = relationshipPropertyProducer.getPropertyValue(nodeId, targetId, random);
                relationshipsImporter.addFromInternal(nodeId, targetId, property);
            }
        }
    }

    static class EmptyRelationshipPropertyProducer implements RelationshipPropertyProducer {
        @Override
        public String getPropertyName() {
            return null;
        }

        @Override
        public double getPropertyValue(long source, long target, java.util.Random random) {
            return 0;
        }
    }
}
