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
import org.neo4j.graphalgo.core.loading.AdjacencyBuilder;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.ImportSizing;
import org.neo4j.graphalgo.core.loading.RelationshipImporter;
import org.neo4j.graphalgo.core.loading.Relationships;
import org.neo4j.graphalgo.core.loading.RelationshipsBatchBuffer;
import org.neo4j.graphalgo.core.loading.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongUnaryOperator;

import static org.neo4j.graphalgo.core.loading.RelationshipsBatchBuffer.BATCH_ENTRY_SIZE;
import static org.neo4j.graphalgo.core.loading.RelationshipsBatchBuffer.SOURCE_ID_OFFSET;
import static org.neo4j.graphalgo.core.loading.RelationshipsBatchBuffer.TARGET_ID_OFFSET;

public final class RandomGraphGenerator {

    private static final int BATCH_SIZE = 10_000;

    private final AllocationTracker allocationTracker;
    private final long nodeCount;
    private final long averageDegree;
    private final Long seed;
    private final RelationshipDistribution relationshipDistribution;
    private final Optional<RelationshipPropertyProducer> maybePropertyProducer;

    public static Graph generate(int nodeCount, int averageDegree) {
        return generate(nodeCount, averageDegree, RelationshipDistribution.POWER_LAW);
    }

    public static Graph generate(int nodeCount, int averageDegree, RelationshipDistribution distribution) {
        return new RandomGraphGenerator(
            nodeCount,
            averageDegree,
            distribution,
            Optional.empty(),
            AllocationTracker.EMPTY
        ).generate();
    }

    public RandomGraphGenerator(
            long nodeCount,
            long averageDegree,
            RelationshipDistribution relationshipDistribution,
            Long seed,
            Optional<RelationshipPropertyProducer> maybePropertyProducer,
            AllocationTracker allocationTracker) {
        this.relationshipDistribution = relationshipDistribution;
        this.maybePropertyProducer = maybePropertyProducer;
        this.allocationTracker = allocationTracker;
        this.nodeCount = nodeCount;
        this.averageDegree = averageDegree;
        this.seed = seed;
    }

    public RandomGraphGenerator(
        long nodeCount,
        long averageDegree,
        RelationshipDistribution relationshipDistribution,
        Optional<RelationshipPropertyProducer> maybePropertyProducer,
        AllocationTracker allocationTracker) {
        this(nodeCount, averageDegree, relationshipDistribution, null, maybePropertyProducer, allocationTracker);
    }

    public HugeGraph generate() {
        IdMap idMap = generateNodes();
        Relationships relationships = generateRelationships(idMap);

        return HugeGraph.create(
                allocationTracker,
                idMap,
                Collections.emptyMap(),
                relationships.relationshipCount(),
                relationships.inAdjacency(),
                relationships.outAdjacency(),
                relationships.inOffsets(),
                relationships.outOffsets(),
                Optional.empty(),
                maybePropertyProducer.map((ignored) -> relationships.inRelProperties()),
                maybePropertyProducer.map((ignored) -> relationships.outRelProperties()),
                maybePropertyProducer.map((ignored) -> relationships.inRelPropertyOffsets()),
                maybePropertyProducer.map((ignored) -> relationships.outRelPropertyOffsets()),
                false
        );
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

    public Long getRelationshipSeed() {
        return seed;
    }

    public Optional<RelationshipPropertyProducer> getMaybePropertyProducer() {
        return maybePropertyProducer;
    }

    public boolean shouldGenerateRelationshipProperty() {
        return maybePropertyProducer.isPresent();
    }

    private IdMap generateNodes() {
        HugeLongArray graphIds = HugeLongArray.newArray(nodeCount, allocationTracker);
        graphIds.fill(0L);

        return new IdMap(
                graphIds,
                null,
                nodeCount
        );
    }

    private Relationships generateRelationships(IdMap idMap) {
        RelImporter relImporter = new RelImporter(
                idMap,
                relationshipDistribution.degreeProducer(nodeCount, averageDegree, seed),
                relationshipDistribution.relationshipProducer(nodeCount, averageDegree, seed),
                maybePropertyProducer);
        return relImporter.generate();
    }

    class RelImporter {

        static final int DUMMY_PROPERTY_ID = 42;

        private final LongUnaryOperator relationshipProducer;
        private final LongUnaryOperator degreeProducer;
        private final Optional<RelationshipPropertyProducer> maybeRelationshipPropertyProducer;
        private final RelationshipsBuilder outRelationshipsBuilder;
        private final RelationshipsBuilder inRelationshipsBuilder;
        private final RelationshipImporter relationshipImporter;
        private final RelationshipImporter.Imports imports;
        private final RelationshipsBatchBuffer relationshipBuffer;
        private long importedRelationships = 0;

        private final boolean shouldGenerateProperty;

        RelImporter(
                IdMap idMap,
                LongUnaryOperator degreeProducer,
                LongUnaryOperator relationshipProducer,
                Optional<RelationshipPropertyProducer> maybeRelationshipPropertyProducer) {
            this.relationshipProducer = relationshipProducer;
            this.degreeProducer = degreeProducer;
            this.maybeRelationshipPropertyProducer = maybeRelationshipPropertyProducer;

            this.shouldGenerateProperty = maybeRelationshipPropertyProducer.isPresent();

            ImportSizing importSizing = ImportSizing.of(1, idMap.nodeCount());
            int pageSize = importSizing.pageSize();
            int numberOfPages = importSizing.numberOfPages();

            this.outRelationshipsBuilder = new RelationshipsBuilder(
                    new DeduplicationStrategy[]{DeduplicationStrategy.NONE},
                    AllocationTracker.EMPTY,
                    shouldGenerateProperty ? 1 : 0);

            this.inRelationshipsBuilder = new RelationshipsBuilder(
                    new DeduplicationStrategy[]{DeduplicationStrategy.NONE},
                    AllocationTracker.EMPTY,
                    shouldGenerateProperty ? 1 : 0);

            int[] relationshipPropertyIds = shouldGenerateProperty ? new int[]{DUMMY_PROPERTY_ID} : new int[0];

            AdjacencyBuilder outAdjacencyBuilder = AdjacencyBuilder.compressing(
                    outRelationshipsBuilder,
                    numberOfPages, pageSize,
                    AllocationTracker.EMPTY,
                    new LongAdder(),
                    relationshipPropertyIds,
                    new double[]{});

            AdjacencyBuilder inAdjacencyBuilder = AdjacencyBuilder.compressing(
                    inRelationshipsBuilder,
                    numberOfPages, pageSize,
                    AllocationTracker.EMPTY,
                    new LongAdder(),
                    relationshipPropertyIds,
                    new double[]{});

            this.relationshipImporter = new RelationshipImporter(
                    AllocationTracker.EMPTY,
                    outAdjacencyBuilder,
                    inAdjacencyBuilder);
            this.imports = relationshipImporter.imports(false, true, true, shouldGenerateProperty);

            relationshipBuffer = new RelationshipsBatchBuffer(idMap, -1, BATCH_SIZE);
        }

        public Relationships generate() {
            long degree, targetId = -1;

            for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
                degree = degreeProducer.applyAsLong(nodeId);

                for (int j = 0; j < degree; j++) {
                    targetId = relationshipProducer.applyAsLong(nodeId);
                    assert (targetId < nodeCount);
                    add(nodeId, targetId);
                }
            }

            flushBuffer();
            return buildRelationships();
        }

        private void add(long source, long target) {
            relationshipBuffer.add(source, target, -1L, -1L);
            if (relationshipBuffer.isFull()) {
                flushBuffer();
                relationshipBuffer.reset();
            }
        }

        private void flushBuffer() {
            RelationshipImporter.PropertyReader relationshipPropertyReader =
                    shouldGenerateProperty ? this::generateRelationshipProperty : null;

            long newImportedInOut = imports.importRels(relationshipBuffer, relationshipPropertyReader);
            importedRelationships += RawValues.getHead(newImportedInOut) / 2;
            relationshipBuffer.reset();
        }

        private Relationships buildRelationships() {
            ParallelUtil.run(relationshipImporter.flushTasks(), null);
            return new Relationships(
                    -1,
                    importedRelationships,
                    inRelationshipsBuilder.adjacency(),
                    outRelationshipsBuilder.adjacency(),
                    inRelationshipsBuilder.globalAdjacencyOffsets(),
                    outRelationshipsBuilder.globalAdjacencyOffsets(),
                    Optional.empty(),
                    shouldGenerateProperty ? inRelationshipsBuilder.weights() : null,
                    shouldGenerateProperty ? outRelationshipsBuilder.weights() : null,
                    shouldGenerateProperty ? inRelationshipsBuilder.globalWeightOffsets() : null,
                    shouldGenerateProperty ? outRelationshipsBuilder.globalWeightOffsets() : null
            );
        }

        private long[][] generateRelationshipProperty(
                long[] batch,
                int batchLength,
                int[] weightProperty,
                double[] defaultWeight) {

            RelationshipPropertyProducer relationshipPropertyProducer = maybeRelationshipPropertyProducer.orElseGet(
                    () -> {
                        throw new UnsupportedOperationException(
                                "Cannot generate relationship properties without a specified generator");
                    }
            );

            long[] weights = new long[batchLength / BATCH_ENTRY_SIZE];
            for (int i = 0; i < batchLength; i += BATCH_ENTRY_SIZE) {
                long source = batch[SOURCE_ID_OFFSET + i];
                long target = batch[TARGET_ID_OFFSET + i];
                double weight = relationshipPropertyProducer.getPropertyValue(source, target);
                weights[i / BATCH_ENTRY_SIZE] = Double.doubleToLongBits(weight);
            }
            return new long[][]{weights};
        }
    }
}
