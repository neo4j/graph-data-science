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
import org.neo4j.counts.CountsStore;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.storageengine.InMemoryDatabaseCreationCatalog;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.recordstorage.AbstractInMemoryRelationshipScanCursor;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.token.TokenHolders;

public final class StorageEngineProxy {

    private static final StorageEngineProxyApi IMPL = ProxyUtil.findProxy(StorageEngineProxyFactory.class);

    private StorageEngineProxy() {}

    public static <ENGINE extends AbstractInMemoryStorageEngine, BUILDER extends InMemoryStorageEngineBuilder<ENGINE>> BUILDER inMemoryStorageEngineBuilder(
        DatabaseLayout databaseLayout,
        TokenHolders tokenHolders
    ) {
        return IMPL.inMemoryStorageEngineBuilder(databaseLayout, tokenHolders);
    }

    public static CountsStore inMemoryCountsStore(GraphStore graphStore, TokenHolders tokenHolders) {
        return IMPL.inMemoryCountsStore(graphStore, tokenHolders);
    }

    public static CommandCreationContext inMemoryCommandCreationContext() {
        return IMPL.inMemoryCommandCreationContext();
    }

    public static void initRelationshipTraversalCursorForRelType(
        StorageRelationshipTraversalCursor cursor,
        long sourceNodeId,
        int relTypeToken
    ) {
        IMPL.initRelationshipTraversalCursorForRelType(cursor, sourceNodeId, relTypeToken);
    }

    public static StorageReader inMemoryStorageReader(
        CypherGraphStore graphStore, TokenHolders tokenHolders, CountsAccessor counts
    ) {
        return IMPL.inMemoryStorageReader(graphStore, tokenHolders, counts);
    }

    public static void createInMemoryDatabase(
        DatabaseManagementService dbms,
        String dbName,
        String graphName,
        Config config
    ) {
        InMemoryDatabaseCreationCatalog.registerDbCreation(dbName, graphName);
        IMPL.createInMemoryDatabase(dbms, dbName, config);
    }

    public static GraphDatabaseService startAndGetInMemoryDatabase(DatabaseManagementService dbms, String dbName) {
        return IMPL.startAndGetInMemoryDatabase(dbms, dbName);
    }

    public static GdsDatabaseManagementServiceBuilder setSkipDefaultIndexesOnCreationSetting(GdsDatabaseManagementServiceBuilder dbmsBuilder) {
        return IMPL.setSkipDefaultIndexesOnCreationSetting(dbmsBuilder);
    }

    public static AbstractInMemoryNodeCursor inMemoryNodeCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        return IMPL.inMemoryNodeCursor(graphStore, tokenHolders);
    }

    public static AbstractInMemoryNodePropertyCursor inMemoryNodePropertyCursor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    ) {
        return IMPL.inMemoryNodePropertyCursor(graphStore, tokenHolders);
    }

    public static AbstractInMemoryRelationshipTraversalCursor inMemoryRelationshipTraversalCursor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    ) {
        return IMPL.inMemoryRelationshipTraversalCursor(graphStore, tokenHolders);
    }

    public static AbstractInMemoryRelationshipScanCursor inMemoryRelationshipScanCursor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    ) {
        return IMPL.inMemoryRelationshipScanCursor(graphStore, tokenHolders);
    }

    public static AbstractInMemoryRelationshipPropertyCursor inMemoryRelationshipPropertyCursor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    ) {
        return IMPL.inMemoryRelationshipPropertyCursor(graphStore, tokenHolders);
    }

    public static void properties(StorageEntityCursor storageCursor, StoragePropertyCursor propertyCursor, int[] propertySelection) {
        IMPL.properties(storageCursor, propertyCursor, propertySelection);
    }

    public static Edition dbmsEdition(GraphDatabaseService databaseService) {
        return IMPL.dbmsEdition(databaseService);
    }
}
