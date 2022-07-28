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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.util.HashMap;
import java.util.Map;

class InMemoryGraphTrackerLifecycleAdapter extends LifecycleAdapter implements DatabaseEventListener {
    private final DatabaseManagementService dbms;
    private final Map<String, DatabaseId> databaseIdMapping;

    InMemoryGraphTrackerLifecycleAdapter(DatabaseManagementService dbms) {
        this.dbms = dbms;
        this.databaseIdMapping = new HashMap<>();
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

    // The @override is missing for compatibility reasons
    public void databaseCreate(DatabaseEventContext eventContext) {
        var databaseName = eventContext.getDatabaseName();
        var db = dbms.database(databaseName);
        databaseIdMapping.put(databaseName, DatabaseId.of(db));
    }

    // The @override is missing for compatibility reasons
    public void databaseDrop(DatabaseEventContext eventContext) {
        databaseIdMapping.remove(eventContext.getDatabaseName());
    }

    private void databaseIsShuttingDown(String databaseName) {
        if (!databaseIdMapping.containsKey(databaseName)) {
            throw new DatabaseNotFoundException(databaseName);
        }
        GraphStoreCatalog.removeAllLoadedGraphs(databaseIdMapping.get(databaseName));
    }

    @Override
    public void databaseStart(DatabaseEventContext eventContext) {

    }

}
