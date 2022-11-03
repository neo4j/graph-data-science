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
package org.neo4j.gds.compat.unsupported;

import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.compat.AbstractInMemoryNodeCursor;
import org.neo4j.gds.compat.AbstractInMemoryNodePropertyCursor;
import org.neo4j.gds.compat.AbstractInMemoryRelationshipPropertyCursor;
import org.neo4j.gds.compat.AbstractInMemoryRelationshipTraversalCursor;
import org.neo4j.gds.compat.GdsDatabaseManagementServiceBuilder;
import org.neo4j.gds.compat.StorageEngineProxyApi;
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

public class StorageEngineProxyImpl implements StorageEngineProxyApi {

    @Override
    public CommandCreationContext inMemoryCommandCreationContext() {
        throw cypherUnsupportedException();
    }

    @Override
    public void initRelationshipTraversalCursorForRelType(
        StorageRelationshipTraversalCursor cursor, long sourceNodeId, int relTypeToken
    ) {
        throw cypherUnsupportedException();
    }

    @Override
    public StorageReader inMemoryStorageReader(
        CypherGraphStore graphStore, TokenHolders tokenHolders, CountsAccessor counts
    ) {
        throw cypherUnsupportedException();
    }

    @Override
    public StorageEngine createInMemoryStorageEngine(DatabaseLayout databaseLayout, TokenHolders tokenHolders) {
        throw cypherUnsupportedException();
    }

    @Override
    public void createInMemoryDatabase(
        DatabaseManagementService dbms,
        String dbName,
        Config config
    ) {
        throw cypherUnsupportedException();
    }

    @Override
    public GraphDatabaseService startAndGetInMemoryDatabase(DatabaseManagementService dbms, String dbName) {
        return null;
    }

    @Override
    public GdsDatabaseManagementServiceBuilder setSkipDefaultIndexesOnCreationSetting(GdsDatabaseManagementServiceBuilder dbmsBuilder) {
        return dbmsBuilder;
    }

    @Override
    public AbstractInMemoryNodeCursor inMemoryNodeCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        throw cypherUnsupportedException();
    }

    @Override
    public AbstractInMemoryNodePropertyCursor inMemoryNodePropertyCursor(
        CypherGraphStore graphStore, TokenHolders tokenHolders
    ) {
        throw cypherUnsupportedException();
    }

    @Override
    public AbstractInMemoryRelationshipTraversalCursor inMemoryRelationshipTraversalCursor(
        CypherGraphStore graphStore, TokenHolders tokenHolders
    ) {
        throw cypherUnsupportedException();
    }

    @Override
    public AbstractInMemoryRelationshipScanCursor inMemoryRelationshipScanCursor(
        CypherGraphStore graphStore, TokenHolders tokenHolders
    ) {
        throw cypherUnsupportedException();
    }

    @Override
    public AbstractInMemoryRelationshipPropertyCursor inMemoryRelationshipPropertyCursor(
        CypherGraphStore graphStore, TokenHolders tokenHolders
    ) {
        throw cypherUnsupportedException();
    }

    @Override
    public void properties(
        StorageEntityCursor storageCursor, StoragePropertyCursor propertyCursor, int[] propertySelection
    ) {
        throw cypherUnsupportedException();
    }

    @Override
    public Edition dbmsEdition(GraphDatabaseService ignored) {
        throw cypherUnsupportedException();
    }

    private UnsupportedOperationException cypherUnsupportedException() {
        return new UnsupportedOperationException("Cypher is not supported for Neo4j versions <4.3.");
    }
}
