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
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;


final class ScanningRelationshipsImporter extends ScanningRecordsImporter<RelationshipRecord, Long> {

    private final GraphSetup setup;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final IdMapping idMap;
    private final RelationshipsBuilder outRelationshipsBuilder;
    private final RelationshipsBuilder inRelationshipsBuilder;
    private final AtomicLong relationshipCounter;

    ScanningRelationshipsImporter(
            GraphSetup setup,
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ImportProgress progress,
            AllocationTracker tracker,
            IdMapping idMap,
            RelationshipsBuilder outRelationshipsBuilder,
            RelationshipsBuilder inRelationshipsBuilder,
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
        this.outRelationshipsBuilder = outRelationshipsBuilder;
        this.inRelationshipsBuilder = inRelationshipsBuilder;
        this.relationshipCounter = new AtomicLong();
    }

    @Override
    InternalImporter.CreateScanner creator(
            final long nodeCount,
            final ImportSizing sizing,
            final AbstractStorePageCacheScanner<RelationshipRecord> scanner) {

        int pageSize = sizing.pageSize();
        int numberOfPages = sizing.numberOfPages();

        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(
                outRelationshipsBuilder,
                numberOfPages, pageSize, tracker, relationshipCounter, dimensions.relWeightId(), setup.relationDefaultWeight);
        AdjacencyBuilder inBuilder = AdjacencyBuilder.compressing(
                inRelationshipsBuilder,
                numberOfPages, pageSize, tracker, relationshipCounter, dimensions.relWeightId(), setup.relationDefaultWeight);

        for (int idx = 0; idx < numberOfPages; idx++) {
            outBuilder.addAdjacencyImporter(tracker, idx);
            inBuilder.addAdjacencyImporter(tracker, idx);
        }

        outBuilder.finishPreparation();
        inBuilder.finishPreparation();

        boolean importWeights = dimensions.relWeightId() != StatementConstants.NO_SUCH_PROPERTY_KEY;

        return RelationshipsScanner.of(
                api, setup, progress, idMap, scanner, dimensions.singleRelationshipTypeId(),
                tracker, importWeights, outBuilder, inBuilder);
    }

    @Override
    Long build() {
        return relationshipCounter.get();
    }
}
