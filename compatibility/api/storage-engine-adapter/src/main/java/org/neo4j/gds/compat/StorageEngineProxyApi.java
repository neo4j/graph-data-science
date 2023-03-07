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
package org.neo4j.gds.compat;

import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.recordstorage.AbstractInMemoryRelationshipScanCursor;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.token.TokenHolders;

import java.util.Locale;

public interface StorageEngineProxyApi {

    static void requireNeo4jVersion(Neo4jVersion version, Class<?> self) {
        if (Neo4jVersion.findNeo4jVersion() != version) {
            throw new IllegalStateException(String.format(
                Locale.ENGLISH,
                "This '%s' instance is only compatible with Neo4j version %s, but Neo4j version %s has been detected.",
                self.getName(),
                version,
                Neo4jVersion.findNeo4jVersion()
            ));
        }
    }

//    InMemoryStorageEngineBuilder<? extends org.neo4j.gds.compat._51.InMemoryStorageEngineImpl> inMemoryStorageEngineBuilder(
//        DatabaseLayout databaseLayout,
//        TokenHolders tokenHolders
//    );
//
//    CountsStore inMemoryCountsStore(GraphStore graphStore, TokenHolders tokenHolders);

    CommandCreationContext inMemoryCommandCreationContext();

    void initRelationshipTraversalCursorForRelType(
        StorageRelationshipTraversalCursor cursor,
        long sourceNodeId,
        int relTypeToken
    );

    StorageReader inMemoryStorageReader(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders,
        CountsAccessor counts
    );

    void createInMemoryDatabase(
        DatabaseManagementService dbms,
        String dbName,
        Config config
    );

    StorageEngine createInMemoryStorageEngine(DatabaseLayout databaseLayout, TokenHolders tokenHolders);

    GraphDatabaseService startAndGetInMemoryDatabase(DatabaseManagementService dbms, String dbName);

    GdsDatabaseManagementServiceBuilder setSkipDefaultIndexesOnCreationSetting(GdsDatabaseManagementServiceBuilder dbmsBuilder);

    AbstractInMemoryNodeCursor inMemoryNodeCursor(CypherGraphStore graphStore, TokenHolders tokenHolders);

    AbstractInMemoryNodePropertyCursor inMemoryNodePropertyCursor(CypherGraphStore graphStore, TokenHolders tokenHolders);

    AbstractInMemoryRelationshipTraversalCursor inMemoryRelationshipTraversalCursor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    );

    AbstractInMemoryRelationshipPropertyCursor inMemoryRelationshipPropertyCursor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    );

    AbstractInMemoryRelationshipScanCursor inMemoryRelationshipScanCursor(CypherGraphStore graphStore, TokenHolders tokenHolders);

    void properties(StorageEntityCursor storageCursor, StoragePropertyCursor propertyCursor, int[] propertySelection);

    Edition dbmsEdition(GraphDatabaseService databaseService);
}
