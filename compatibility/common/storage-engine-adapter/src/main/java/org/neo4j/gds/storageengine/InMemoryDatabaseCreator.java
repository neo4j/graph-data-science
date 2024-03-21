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

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.StorageEngineProxy;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;

import java.util.concurrent.CountDownLatch;

import static org.neo4j.gds.core.cypher.CypherGraphStoreCatalogHelper.setWrappedGraphStore;

public final class InMemoryDatabaseCreator {

    private InMemoryDatabaseCreator() {}

    public static void createDatabase(
        GraphDatabaseService databaseService,
        String username,
        String graphName,
        String dbName
    ) {
        createDatabase(databaseService, username, graphName, dbName, Config.defaults());
    }

    public static void createDatabase(
        GraphDatabaseService databaseService,
        String username,
        String graphName,
        String dbName,
        Config config
    ) {
        try {
            var dbms = GraphDatabaseApiProxy.resolveDependency(databaseService, DatabaseManagementService.class);
            var graphStoreWithConfig = GraphStoreCatalog.get(CatalogRequest.of(username, databaseService.databaseName()), graphName);
            var cypherGraphStore = new CypherGraphStore(graphStoreWithConfig.graphStore());
            setWrappedGraphStore(graphStoreWithConfig.config(), cypherGraphStore);
            createAndAwaitDatabase(dbms, dbName, graphName, config);
        } catch (Exception e) {
            InMemoryDatabaseCreationCatalog.removeDatabaseEntry(dbName);
            throw e;
        }
    }

    private static void createAndAwaitDatabase(DatabaseManagementService dbms, String dbName, String graphName, Config config) {
        var databaseCreationLatch = new CountDownLatch(1);
        dbms.registerDatabaseEventListener(new DatabaseEventListener() {
            @Override
            public void databaseStart(DatabaseEventContext eventContext) {
                if (eventContext.getDatabaseName().equals(dbName)) {
                    databaseCreationLatch.countDown();
                }
            }

            @Override
            public void databaseShutdown(DatabaseEventContext eventContext) {

            }

            @Override
            public void databasePanic(DatabaseEventContext eventContext) {
                if (eventContext.getDatabaseName().equals(dbName)) {
                    databaseCreationLatch.countDown();
                }
            }

            @Override
            public void databaseCreate(DatabaseEventContext eventContext) {

            }

            @Override
            public void databaseDrop(DatabaseEventContext eventContext) {

            }
        });

        StorageEngineProxy.createInMemoryDatabase(dbms, dbName, graphName, config);
        try {
            databaseCreationLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
