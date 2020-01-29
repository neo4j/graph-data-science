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

import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

final class RelationshipsScanner extends StatementAction implements RecordScanner {

    static InternalImporter.CreateScanner of(
            GraphDatabaseAPI api,
            GraphSetup setup,
            ImportProgress progress,
            IdMapping idMap,
            AbstractStorePageCacheScanner<RelationshipRecord> scanner,
            boolean loadWeights,
            Collection<SingleTypeRelationshipImporter.Builder> importerBuilders) {
        List<SingleTypeRelationshipImporter.Builder.WithImporter> builders = importerBuilders
                .stream()
                .map(relImporter -> {
                    RelationshipProjectionMapping relationshipProjectionMapping = relImporter.mapping();
                    return relImporter.loadImporter(
                        relationshipProjectionMapping.projection() == Projection.UNDIRECTED,
                        relationshipProjectionMapping.projection() == Projection.NATURAL,
                        relationshipProjectionMapping.projection() == Projection.REVERSE,
                        loadWeights
                    );
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (builders.isEmpty()) {
            return InternalImporter.createEmptyScanner();
        }
        return new RelationshipsScanner.Creator(
                api,
                progress,
                idMap,
                scanner,
                builders,
            setup.terminationFlag()
        );
    }

    static final class Creator implements InternalImporter.CreateScanner {
        private final GraphDatabaseAPI api;
        private final ImportProgress progress;
        private final IdMapping idMap;
        private final AbstractStorePageCacheScanner<RelationshipRecord> scanner;
        private final List<SingleTypeRelationshipImporter.Builder.WithImporter> importerBuilders;
        private final TerminationFlag terminationFlag;

        Creator(
                GraphDatabaseAPI api,
                ImportProgress progress,
                IdMapping idMap,
                AbstractStorePageCacheScanner<RelationshipRecord> scanner,
                List<SingleTypeRelationshipImporter.Builder.WithImporter> importerBuilders,
                TerminationFlag terminationFlag) {
            this.api = api;
            this.progress = progress;
            this.idMap = idMap;
            this.scanner = scanner;
            this.importerBuilders = importerBuilders;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public RecordScanner create(final int index) {
            return new RelationshipsScanner(
                    api,
                    terminationFlag,
                    progress,
                    idMap,
                    scanner,
                    index,
                    importerBuilders
            );
        }

        @Override
        public Collection<Runnable> flushTasks() {
            return importerBuilders.stream()
                    .flatMap(SingleTypeRelationshipImporter.Builder.WithImporter::flushTasks)
                    .collect(Collectors.toList());
        }
    }

    private final TerminationFlag terminationFlag;
    private final ImportProgress progress;
    private final IdMapping idMap;
    private final AbstractStorePageCacheScanner<RelationshipRecord> scanner;
    private final int scannerIndex;
    private final List<SingleTypeRelationshipImporter.Builder.WithImporter> importerBuilders;

    private long relationshipsImported;
    private long weightsImported;

    private RelationshipsScanner(
            GraphDatabaseAPI api,
            TerminationFlag terminationFlag,
            ImportProgress progress,
            IdMapping idMap,
            AbstractStorePageCacheScanner<RelationshipRecord> scanner,
            int threadIndex,
            List<SingleTypeRelationshipImporter.Builder.WithImporter> importerBuilders) {
        super(api);
        this.terminationFlag = terminationFlag;
        this.progress = progress;
        this.idMap = idMap;
        this.scanner = scanner;
        this.scannerIndex = threadIndex;
        this.importerBuilders = importerBuilders;
    }

    @Override
    public String threadName() {
        return "relationship-store-scan-" + scannerIndex;
    }

    @Override
    public void accept(final KernelTransaction transaction) {
        scanRelationships(transaction.dataRead(), transaction.cursors());
    }

    private void scanRelationships(final Read read, final CursorFactory cursors) {
        try (AbstractStorePageCacheScanner<RelationshipRecord>.Cursor cursor = scanner.getCursor()) {
            List<SingleTypeRelationshipImporter> importers = this.importerBuilders.stream()
                    .map(imports -> imports.withBuffer(idMap, cursor.bulkSize(), read, cursors))
                    .collect(Collectors.toList());

            RelationshipsBatchBuffer[] buffers = importers
                    .stream()
                    .map(SingleTypeRelationshipImporter::buffer)
                    .toArray(RelationshipsBatchBuffer[]::new);

            RecordsBatchBuffer<RelationshipRecord> buffer = CompositeRelationshipsBatchBuffer.of(buffers);

            long allImportedRels = 0L;
            long allImportedWeights = 0L;
            while (buffer.scan(cursor)) {
                terminationFlag.assertRunning();
                long imported = 0L;
                for (SingleTypeRelationshipImporter importer : importers) {
                    imported += importer.importRels();
                }
                int importedRels = RawValues.getHead(imported);
                int importedWeights = RawValues.getTail(imported);
                progress.relationshipsImported(importedRels);
                allImportedRels += importedRels;
                allImportedWeights += importedWeights;
            }
            relationshipsImported = allImportedRels;
            weightsImported = allImportedWeights;
        }
    }

    @Override
    public long propertiesImported() {
        return weightsImported;
    }

    @Override
    public long recordsImported() {
        return relationshipsImported;
    }

}
