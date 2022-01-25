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

import org.immutables.builder.Builder;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.compress.AdjacencyFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.utils.GdsFeatureToggles.USE_PRE_AGGREGATION;


public final class ScanningRelationshipsImporter extends ScanningRecordsImporter<RelationshipReference, RelationshipsAndProperties> {

    private final GraphProjectConfig graphProjectConfig;
    private final GraphLoaderContext loadingContext;

    private final IdMap idMap;
    private final Map<RelationshipType, AdjacencyListWithPropertiesBuilder> adjacencyListBuilders;

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
    }

    @Override
    public RecordScannerTaskRunner.RecordScannerTaskFactory recordScannerTaskFactory(
        long nodeCount,
        ImportSizing sizing,
        StoreScanner<RelationshipReference> storeScanner
    ) {
        var importerBuilders = adjacencyListBuilders
            .entrySet()
            .stream()
            .map(entry -> new SingleTypeRelationshipImporterBuilderBuilder()
                .adjacencyListWithPropertiesBuilder(entry.getValue())
                .typeToken(dimensions.relationshipTypeTokenMapping().get(entry.getKey()))
                .projection(entry.getValue().projection())
                .importSizing(sizing)
                .validateRelationships(graphProjectConfig.validateRelationships())
                .preAggregate(USE_PRE_AGGREGATION.isEnabled())
                .allocationTracker(allocationTracker)
                .build())
            .collect(Collectors.toList());

        return RelationshipsScannerTask.factory(
            loadingContext,
            progressTracker,
            idMap,
            storeScanner,
            importerBuilders
        );
    }

    @Override
    public RelationshipsAndProperties build() {
        return RelationshipsAndProperties.of(adjacencyListBuilders);
    }
}
