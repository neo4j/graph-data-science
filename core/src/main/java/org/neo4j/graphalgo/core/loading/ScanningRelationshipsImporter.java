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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;


final class ScanningRelationshipsImporter extends ScanningRecordsImporter<RelationshipRecord, ObjectLongMap<RelationshipProjectionMapping>> {

    private final GraphSetup setup;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final IdMapping idMap;
    private final Map<RelationshipProjectionMapping, RelationshipsBuilder> allBuilders;
    private final Map<RelationshipProjectionMapping, LongAdder> allRelationshipCounters;

    private boolean validateRelationships;

    ScanningRelationshipsImporter(
        GraphSetup setup,
        GraphDatabaseAPI api,
        GraphDimensions dimensions,
        ImportProgress progress,
        AllocationTracker tracker,
        IdMapping idMap,
        Map<RelationshipProjectionMapping, RelationshipsBuilder> allBuilders,
        ExecutorService threadPool,
        int concurrency,
        boolean validateRelationships
    ) {
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
        this.validateRelationships = validateRelationships;
        this.allRelationshipCounters = new HashMap<>();
    }

    @Override
    InternalImporter.CreateScanner creator(
            final long nodeCount,
            final ImportSizing sizing,
            final AbstractStorePageCacheScanner<RelationshipRecord> scanner) {

        int pageSize = sizing.pageSize();
        int numberOfPages = sizing.numberOfPages();

        boolean importWeights = dimensions.relationshipProperties().atLeastOneExists();

        List<SingleTypeRelationshipImporter.Builder> importerBuilders = allBuilders
                .entrySet()
                .stream()
                .map(entry -> createImporterBuilder(pageSize, numberOfPages, entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        for (SingleTypeRelationshipImporter.Builder importerBuilder : importerBuilders) {
            allRelationshipCounters.put(importerBuilder.mapping(), importerBuilder.relationshipCounter());
        }

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
            RelationshipProjectionMapping mapping,
            RelationshipsBuilder relationshipsBuilder
    ) {
        int[] propertyKeyIds = dimensions.relationshipProperties().allPropertyKeyIds();
        double[] defaultValues = dimensions.relationshipProperties().allDefaultValues();

        LongAdder relationshipCounter = new LongAdder();
        AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
                relationshipsBuilder,
                numberOfPages,
                pageSize,
                tracker,
                relationshipCounter,
                propertyKeyIds,
                defaultValues
        );

        RelationshipImporter importer = new RelationshipImporter(setup.tracker(), adjacencyBuilder);
        return new SingleTypeRelationshipImporter.Builder(mapping, importer, relationshipCounter, validateRelationships);
    }

    @Override
    ObjectLongMap<RelationshipProjectionMapping> build() {
        ObjectLongMap<RelationshipProjectionMapping> relationshipCounters = new ObjectLongHashMap<>(allRelationshipCounters.size());
        allRelationshipCounters.forEach((mapping, counter) -> relationshipCounters.put(mapping, counter.sum()));
        return relationshipCounters;
    }
}
