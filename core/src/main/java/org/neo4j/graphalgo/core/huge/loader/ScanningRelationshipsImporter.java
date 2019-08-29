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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


final class ScanningRelationshipsImporter extends ScanningRecordsImporter<RelationshipRecord, Long> {

    private final GraphSetup setup;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final IdMapping idMap;
    private final Map<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders;
    private final AtomicLong relationshipCounter;

    ScanningRelationshipsImporter(
            GraphSetup setup,
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ImportProgress progress,
            AllocationTracker tracker,
            IdMapping idMap,
            Map<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders,
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
        this.allBuilders = allBuilders;
        this.relationshipCounter = new AtomicLong();
    }

    @Override
    InternalImporter.CreateScanner creator(
            final long nodeCount,
            final ImportSizing sizing,
            final AbstractStorePageCacheScanner<RelationshipRecord> scanner) {

        int pageSize = sizing.pageSize();
        int numberOfPages = sizing.numberOfPages();

        boolean importWeights = dimensions.relWeightId() != StatementConstants.NO_SUCH_PROPERTY_KEY;

        List<SingleTypeRelationshipImporter.Builder> importerBuilders = allBuilders
                .entrySet()
                .stream()
                .map(entry -> createImporterBuilder(pageSize, numberOfPages, entry))
                .collect(Collectors.toList());

        return RelationshipsScanner.of(
                api,
                setup,
                progress,
                idMap,
                scanner,
                importWeights,
                importerBuilders
        );
    }

    private SingleTypeRelationshipImporter.Builder createImporterBuilder(
            int pageSize,
            int numberOfPages,
            Map.Entry<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> entry) {
        RelationshipTypeMapping mapping = entry.getKey();
        RelationshipsBuilder outRelationshipsBuilder = entry.getValue().getLeft();
        RelationshipsBuilder inRelationshipsBuilder = entry.getValue().getRight();
        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(
                outRelationshipsBuilder,
                numberOfPages,
                pageSize,
                tracker,
                relationshipCounter,
                dimensions.relWeightId(),
                setup.relationDefaultWeight);
        AdjacencyBuilder inBuilder = AdjacencyBuilder.compressing(
                inRelationshipsBuilder,
                numberOfPages,
                pageSize,
                tracker,
                relationshipCounter,
                dimensions.relWeightId(),
                setup.relationDefaultWeight);

        RelationshipImporter importer = new RelationshipImporter(
                setup.tracker,
                outBuilder,
                setup.loadAsUndirected ? outBuilder : inBuilder
        );

        return new SingleTypeRelationshipImporter.Builder(mapping, importer);
    }

    @Override
    Long build() {
        return relationshipCounter.get();
    }
}
