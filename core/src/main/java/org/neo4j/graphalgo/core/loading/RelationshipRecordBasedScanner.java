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

import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

final class RelationshipRecordBasedScanner extends AbstractRecordBasedScanner<RelationshipReference, RelationshipRecord, RelationshipStore> {

    static final StoreScanner.Factory<RelationshipReference> FACTORY = RelationshipRecordBasedScanner::new;

    private RelationshipRecordBasedScanner(int prefetchSize, SecureTransaction transaction) {
        super(prefetchSize, transaction);
    }

    @Override
    public RelationshipStore store(NeoStores neoStores) {
        return neoStores.getRelationshipStore();
    }

    @Override
    public RecordFormat<RelationshipRecord> recordFormat(RecordFormats formats) {
        return formats.relationship();
    }

    @Override
    public RelationshipReference recordReference(
        RelationshipRecord record,
        RelationshipStore store
    ) {
        return new RelationshipRecordReference(record);
    }

    @Override
    public String storeFileName() {
        return DatabaseFile.RELATIONSHIP_STORE.getName();
    }
}
