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
import org.neo4j.graphalgo.core.huge.loader.RelationshipImporter.Imports;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;

final class RelationshipsScanner extends StatementAction implements RecordScanner {

    static InternalImporter.CreateScanner of(
            GraphDatabaseAPI api,
            GraphSetup setup,
            ImportProgress progress,
            IdMapping idMap,
            AbstractStorePageCacheScanner<RelationshipRecord> scanner,
            int relType,
            boolean loadWeights,
            RelationshipImporter importer) {
        Imports imports = RelationshipImporter.imports(
                importer,
                setup.loadAsUndirected,
                setup.loadOutgoing,
                setup.loadIncoming,
                loadWeights);
        if (imports == null) {
            return InternalImporter.createEmptyScanner();
        }
        return new RelationshipsScanner.Creator(
                api,
                progress,
                idMap,
                scanner,
                relType,
                importer,
                imports,
                setup.terminationFlag
        );
    }

    static final class Creator implements InternalImporter.CreateScanner {
        private final GraphDatabaseAPI api;
        private final ImportProgress progress;
        private final IdMapping idMap;
        private final AbstractStorePageCacheScanner<RelationshipRecord> scanner;
        private final int relType;
        private final RelationshipImporter importer;
        private final Imports imports;
        private final TerminationFlag terminationFlag;

        Creator(
                GraphDatabaseAPI api,
                ImportProgress progress,
                IdMapping idMap,
                AbstractStorePageCacheScanner<RelationshipRecord> scanner,
                int relType,
                RelationshipImporter importer,
                Imports imports,
                TerminationFlag terminationFlag) {
            this.api = api;
            this.progress = progress;
            this.idMap = idMap;
            this.scanner = scanner;
            this.relType = relType;
            this.importer = importer;
            this.imports = imports;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public RecordScanner create(final int index) {
            return new RelationshipsScanner(api,
                    terminationFlag,
                    progress,
                    idMap,
                    scanner,
                    relType,
                    index,
                    importer,
                    imports
            );
        }

        @Override
        public Collection<Runnable> flushTasks() {
            return importer.flushTasks();
        }
    }

    private final TerminationFlag terminationFlag;
    private final ImportProgress progress;
    private final IdMapping idMap;
    private final AbstractStorePageCacheScanner<RelationshipRecord> scanner;
    private final int relType;
    private final int scannerIndex;
    private final RelationshipImporter importer;
    private final Imports imports;

    private long relationshipsImported;
    private long weightsImported;

    private RelationshipsScanner(
            GraphDatabaseAPI api,
            TerminationFlag terminationFlag,
            ImportProgress progress,
            IdMapping idMap,
            AbstractStorePageCacheScanner<RelationshipRecord> scanner,
            int relType,
            int threadIndex,
            RelationshipImporter importer,
            Imports imports) {
        super(api);
        this.terminationFlag = terminationFlag;
        this.progress = progress;
        this.idMap = idMap;
        this.scanner = scanner;
        this.relType = relType;
        this.scannerIndex = threadIndex;
        this.importer = importer;
        this.imports = imports;
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
            RelationshipsBatchBuffer batches = new RelationshipsBatchBuffer(idMap, relType, cursor.bulkSize());

            final ImportProgress progress = this.progress;
            final Imports imports = this.imports;

            RelationshipImporter.WeightReader weightReader = importer.storeBackedWeightReader(cursors, read);

            long allImportedRels = 0L;
            long allImportedWeights = 0L;
            while (batches.scan(cursor)) {
                terminationFlag.assertRunning();
                long imported = imports.importRels(batches, weightReader);
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
