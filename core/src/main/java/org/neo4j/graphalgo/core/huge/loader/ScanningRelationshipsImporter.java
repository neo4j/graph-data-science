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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;


final class ScanningRelationshipsImporter extends ScanningRecordsImporter<RelationshipRecord, Long> {

    private final GraphSetup setup;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final IdMapping idMap;
    private final HugeWeightMapBuilder weights;
    private final boolean loadDegrees;
    private final HugeAdjacencyBuilder outAdjacency;
    private final HugeAdjacencyBuilder inAdjacency;
    private final AtomicLong relationshipCounter;

    ScanningRelationshipsImporter(
            GraphSetup setup,
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ImportProgress progress,
            AllocationTracker tracker,
            IdMapping idMap,
            HugeWeightMapBuilder weights,
            boolean loadDegrees,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            ExecutorService threadPool,
            int concurrency) {
        super(
                RelationshipStoreScanner.RELATIONSHIP_ACCESS,
                "Relationship",
                api,
                dimensions,
                threadPool,
                concurrency);
        this.setup = setup;
        this.progress = progress;
        this.tracker = tracker;
        this.idMap = idMap;
        this.weights = weights;
        this.loadDegrees = loadDegrees;
        this.outAdjacency = outAdjacency;
        this.inAdjacency = inAdjacency;
        this.relationshipCounter = new AtomicLong();
    }

    @Override
    InternalImporter.CreateScanner creator(
            final long nodeCount,
            final ImportSizing sizing,
            final AbstractStorePageCacheScanner<RelationshipRecord> scanner) {

        int pageSize = sizing.pageSize();
        int numberOfPages = sizing.numberOfPages();

        WeightBuilder weightBuilder = WeightBuilder.of(weights, numberOfPages, pageSize, nodeCount, tracker);
        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(outAdjacency, numberOfPages, pageSize, tracker, relationshipCounter);
        AdjacencyBuilder inBuilder = AdjacencyBuilder.compressing(inAdjacency, numberOfPages, pageSize, tracker, relationshipCounter);

        for (int idx = 0; idx < numberOfPages; idx++) {
            weightBuilder.addWeightImporter(idx);
            outBuilder.addAdjacencyImporter(tracker, loadDegrees, idx);
            inBuilder.addAdjacencyImporter(tracker, loadDegrees, idx);
        }

        weightBuilder.finish();
        outBuilder.finishPreparation();
        inBuilder.finishPreparation();

        return RelationshipsScanner.of(
                api, setup, progress, idMap, scanner, dimensions.singleRelationshipTypeId(),
                tracker, weightBuilder, outBuilder, inBuilder);
    }

    @Override
    Long build() {
        return relationshipCounter.get();
    }
}
