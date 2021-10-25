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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class InMemoryGraphTrackerLifecycleAdapter extends LifecycleAdapter implements DatabaseEventListener {
    private final DatabaseManagementService dbms;
    private final DatabaseManager<DatabaseContext> databaseManager;

    InMemoryGraphTrackerLifecycleAdapter(
        DatabaseManagementService dbms,
        DatabaseManager<DatabaseContext> databaseManager
    ) {
        this.dbms = dbms;
        this.databaseManager = databaseManager;
    }

    @Override
    public void init() throws Exception {
        dbms.registerDatabaseEventListener(this);
    }

    @Override
    public void shutdown() throws Exception {
        dbms.unregisterDatabaseEventListener(this);
    }

    @Override
    public void databaseShutdown(DatabaseEventContext eventContext) {
        databaseIsShuttingDown(eventContext.getDatabaseName());
    }

    @Override
    public void databasePanic(DatabaseEventContext eventContext) {
        databaseIsShuttingDown(eventContext.getDatabaseName());
    }

    private void databaseIsShuttingDown(String databaseName) {
        var databaseIds = databaseManager.registeredDatabases().keySet();
        var namedDatabaseId = databaseIds
            .stream()
            .filter(id -> id.name().equals(databaseName))
            .findFirst()
            .orElseThrow(() -> new DatabaseNotFoundException(databaseName));

        GraphStoreCatalog.removeAllLoadedGraphs(namedDatabaseId);
    }

    @Override
    public void databaseStart(DatabaseEventContext eventContext) {

    }

}
