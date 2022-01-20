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
package org.neo4j.gds.core.loading;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import org.immutables.builder.Builder;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.compress.AdjacencyFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.utils.GdsFeatureToggles.USE_PRE_AGGREGATION;


public final class ScanningRelationshipsImporter extends ScanningRecordsImporter<RelationshipReference, RelationshipsAndProperties> {

    private final GraphProjectConfig graphProjectConfig;
    private final GraphLoaderContext loadingContext;

    private final IdMap idMap;
    private final Map<RelationshipType, AdjacencyListWithPropertiesBuilder> adjacencyListBuilders;
    private final Map<RelationshipType, LongAdder> allRelationshipCounters;

    @Builder.Factory
    public static ScanningRelationshipsImporter scanningRelationshipsImporter(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker,
        IdMap idMap,
        int concurrency
    ) {
        Map<RelationshipType, AdjacencyListWithPropertiesBuilder> adjacencyListBuilders = graphProjectConfig
            .relationshipProjections()
            .projections()
            .entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                projectionEntry -> AdjacencyListWithPropertiesBuilder.create(
                    dimensions::nodeCount,
                    AdjacencyFactory.configured(),
                    projectionEntry.getValue(),
                    dimensions.relationshipPropertyTokens(),
                    loadingContext.allocationTracker()
                )
            ));

        return new ScanningRelationshipsImporter(
            graphProjectConfig,
            loadingContext,
            dimensions,
            progressTracker,
            idMap,
            adjacencyListBuilders,
            concurrency
        );
    }

    private ScanningRelationshipsImporter(
        GraphProjectConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker,
        IdMap idMap,
        Map<RelationshipType, AdjacencyListWithPropertiesBuilder> adjacencyListBuilders,
        int concurrency
    ) {
        super(
            RelationshipScanCursorBasedScanner.FACTORY,
            loadingContext,
            dimensions,
            progressTracker,
            concurrency
        );
        this.graphProjectConfig = graphProjectConfig;
        this.loadingContext = loadingContext;
        this.idMap = idMap;
        this.adjacencyListBuilders = adjacencyListBuilders;
        this.allRelationshipCounters = new HashMap<>();
    }

    @Override
    public RecordScannerTaskRunner.RecordScannerTaskFactory recordScannerTaskFactory(
        long nodeCount,
        ImportSizing sizing,
        StoreScanner<RelationshipReference> storeScanner
    ) {

        int pageSize = sizing.pageSize();
        int numberOfPages = sizing.numberOfPages();

        List<SingleTypeRelationshipImporter.Builder> importerBuilders = adjacencyListBuilders
            .entrySet()
            .stream()
            .map(entry -> {
                var relationshipType = entry.getKey();
                var relationshipsBuilder = entry.getValue();
                return createImporterBuilder(
                    pageSize,
                    numberOfPages,
                    relationshipType,
                    relationshipsBuilder.projection(),
                    relationshipsBuilder
                );
            })
            .collect(Collectors.toList());

        for (SingleTypeRelationshipImporter.Builder importerBuilder : importerBuilders) {
            allRelationshipCounters.put(importerBuilder.relationshipType(), importerBuilder.relationshipCounter());
        }

        return RelationshipsScannerTask.factory(
            loadingContext,
            progressTracker,
            idMap,
            storeScanner,
            importerBuilders
        );
    }

    private SingleTypeRelationshipImporter.Builder createImporterBuilder(
        int pageSize,
        int numberOfPages,
        RelationshipType relationshipType,
        RelationshipProjection projection,
        @NotNull AdjacencyListWithPropertiesBuilder adjacencyListWithPropertiesBuilder
    ) {
        LongAdder relationshipCounter = new LongAdder();
        AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
            adjacencyListWithPropertiesBuilder,
            numberOfPages,
            pageSize,
            allocationTracker,
            relationshipCounter,
            USE_PRE_AGGREGATION.isEnabled()
        );

        RelationshipImporter importer = new RelationshipImporter(loadingContext.allocationTracker(), adjacencyBuilder);
        int typeId = dimensions.relationshipTypeTokenMapping().get(relationshipType);
        return new SingleTypeRelationshipImporter.Builder(
            relationshipType,
            projection,
            typeId,
            importer,
            relationshipCounter,
            graphProjectConfig.validateRelationships()
        );
    }

    @Override
    public RelationshipsAndProperties build() {
        ObjectLongMap<RelationshipType> relationshipCounters = new ObjectLongHashMap<>(allRelationshipCounters.size());
        allRelationshipCounters.forEach((relationshipType, counter) -> relationshipCounters.put(
            relationshipType,
            counter.sum()
        ));

        return RelationshipsAndProperties.of(relationshipCounters, adjacencyListBuilders);
    }
}
