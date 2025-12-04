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
package org.neo4j.gds.procedures.integration;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;

import java.util.HashMap;
import java.util.Map;

class InMemoryGraphTrackerDatabaseEventListener extends DatabaseEventListenerAdapter {
    private final Map<String, DatabaseId> databaseIdMapping = new HashMap<>();

    private final DatabaseManagementService databaseManagementService;
    private final GraphStoreCatalogService graphStoreCatalogService;

    InMemoryGraphTrackerDatabaseEventListener(
        DatabaseManagementService databaseManagementService,
        GraphStoreCatalogService graphStoreCatalogService
    ) {
        this.databaseManagementService = databaseManagementService;
        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    @Override
    public void databaseCreate(DatabaseEventContext eventContext) {
        var databaseName = eventContext.getDatabaseName();
        var database = databaseManagementService.database(databaseName);

        databaseIdMapping.put(databaseName, DatabaseId.of(database.databaseName()));
    }

    @Override
    public void databaseDrop(DatabaseEventContext eventContext) {
        databaseIdMapping.remove(eventContext.getDatabaseName());
    }

    @Override
    public void databaseShutdown(DatabaseEventContext eventContext) {
        databaseIsShuttingDown(eventContext.getDatabaseName());
    }

    @Override
    public void databasePanic(DatabaseEventContext eventContext) {
        databaseIsShuttingDown(eventContext.getDatabaseName());
    }

    /**
     * React to a database shutdown or panic by clearing out the graphs associated with that database.
     */
    private void databaseIsShuttingDown(String databaseName) {
        if (!databaseIdMapping.containsKey(databaseName)) return;

        graphStoreCatalogService.removeAllLoadedGraphs(databaseIdMapping.get(databaseName));
    }
}
