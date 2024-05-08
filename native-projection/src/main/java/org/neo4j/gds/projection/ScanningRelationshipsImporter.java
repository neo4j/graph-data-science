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
package org.neo4j.gds.projection;

import org.immutables.builder.Builder;
import org.neo4j.gds.ImmutableRelationshipProjection;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.ImmutableSingleTypeRelationshipImportContext;
import org.neo4j.gds.core.loading.ImportSizing;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporter;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporter.SingleTypeRelationshipImportContext;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporterBuilder;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

final class ScanningRelationshipsImporter extends ScanningRecordsImporter<RelationshipReference, RelationshipImportResult> {

    private final GraphProjectFromStoreConfig graphProjectConfig;
    private final GraphLoaderContext loadingContext;

    private final IdMap idMap;
    private List<SingleTypeRelationshipImportContext> importContexts;

    @Builder.Factory
    static ScanningRelationshipsImporter scanningRelationshipsImporter(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker,
        IdMap idMap,
        Concurrency concurrency
    ) {
        return new ScanningRelationshipsImporter(
            graphProjectConfig,
            loadingContext,
            dimensions,
            progressTracker,
            idMap,
            concurrency
        );
    }

    private ScanningRelationshipsImporter(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker,
        IdMap idMap,
        Concurrency concurrency
    ) {
        super(
            RelationshipScanCursorBasedScanner.factory(Math.max(dimensions.relationshipCounts()
                .values()
                .stream()
                .mapToLong(Long::longValue)
                .sum(), dimensions.relCountUpperBound())),
            loadingContext,
            dimensions,
            progressTracker,
            concurrency
        );
        this.graphProjectConfig = graphProjectConfig;
        this.loadingContext = loadingContext;
        this.idMap = idMap;
    }

    @Override
    public RecordScannerTaskRunner.RecordScannerTaskFactory recordScannerTaskFactory(
        long nodeCount,
        ImportSizing sizing,
        StoreScanner<RelationshipReference> storeScanner
    ) {
        this.importContexts = graphProjectConfig
            .relationshipProjections()
            .projections()
            .entrySet()
            .stream()
            .flatMap(
                entry -> {
                    var relationshipType = entry.getKey();
                    var projection = entry.getValue();

                    var importMetaData = SingleTypeRelationshipImporter.ImportMetaData.of(
                        projection,
                        dimensions.relationshipTypeTokenMapping().get(relationshipType),
                        dimensions.relationshipPropertyTokens(),
                        !graphProjectConfig.validateRelationships()
                    );

                    var importer = new SingleTypeRelationshipImporterBuilder()
                        .importMetaData(importMetaData)
                        .nodeCountSupplier(dimensions::nodeCount)
                        .importSizing(sizing)
                        .build();

                    var contexts = new ArrayList<SingleTypeRelationshipImportContext>();

                    contexts.add(ImmutableSingleTypeRelationshipImportContext.builder()
                        .relationshipType(relationshipType)
                        .relationshipProjection(projection)
                        .singleTypeRelationshipImporter(importer)
                        .build());

                    if (projection.indexInverse()) {
                        contexts.add(createInverseImporterContext(sizing, relationshipType, projection));
                    }

                    return contexts.stream();
                }
            ).collect(Collectors.toList());

        return RelationshipsScannerTask.factory(
            loadingContext,
            progressTracker,
            idMap,
            storeScanner,
            this.importContexts
                .stream()
                .map(SingleTypeRelationshipImportContext::singleTypeRelationshipImporter)
                .collect(Collectors.toList())
        );
    }

    private SingleTypeRelationshipImportContext createInverseImporterContext(
        ImportSizing sizing,
        RelationshipType relationshipType,
        RelationshipProjection projection
    ) {
        var inverseProjection = ImmutableRelationshipProjection
            .builder()
            .from(projection)
            .orientation(projection.orientation().inverse())
            .build();

        var inverseImportMetaData = SingleTypeRelationshipImporter.ImportMetaData.of(
            inverseProjection,
            dimensions.relationshipTypeTokenMapping().get(relationshipType),
            dimensions.relationshipPropertyTokens(),
            !graphProjectConfig.validateRelationships()
        );

        var inverseImporter = new SingleTypeRelationshipImporterBuilder()
            .importMetaData(inverseImportMetaData)
            .nodeCountSupplier(dimensions::nodeCount)
            .importSizing(sizing)
            .build();

        return ImmutableSingleTypeRelationshipImportContext.builder()
            .relationshipType(relationshipType)
            .relationshipProjection(inverseProjection)
            .inverseOfRelationshipType(relationshipType)
            .singleTypeRelationshipImporter(inverseImporter)
            .build();
    }

    @Override
    public RelationshipImportResult build() {
        return RelationshipImportResult.of(importContexts);
    }
}
