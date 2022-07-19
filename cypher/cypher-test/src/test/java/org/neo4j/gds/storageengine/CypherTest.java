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
package org.neo4j.gds.storageengine;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.internal.recordstorage.InMemoryStorageEngineCompanion;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

public abstract class CypherTest extends BaseTest {

    protected CypherGraphStore graphStore;
    protected TokenHolders tokenHolders;
    protected StorageEngine storageEngine;

    @BeforeEach
    void setup() throws Exception {
        this.graphStore = new CypherGraphStore(graphStore());

        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("", GraphDatabaseApiProxy.databaseLayout(db).getDatabaseName()), graphStore);

        this.tokenHolders = new TokenHolders(
            new DelegatingTokenHolder(new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY),
            new DelegatingTokenHolder(new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL),
            new DelegatingTokenHolder(new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE)
        );

        this.storageEngine = InMemoryStorageEngineCompanion.create(GraphDatabaseApiProxy.databaseLayout(db), tokenHolders);
        this.storageEngine.schemaAndTokensLifecycle().init();
        onSetup();
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
        InMemoryDatabaseCreationCatalog.removeAllRegisteredDbCreations();
    }

    protected abstract GraphStore graphStore();

    protected abstract void onSetup();
}
