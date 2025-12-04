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
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Track the lifecycle of the DBMS, and use the hooks to attach a database event listener,
 * so that we can manage the state of the in-memory graphs that match the different databases in the DBMS.
 */
class InMemoryGraphTracker extends LifecycleAdapter {
    private final DatabaseManagementService databaseManagementService;
    private final InMemoryGraphTrackerDatabaseEventListener inMemoryGraphTrackerDatabaseEventListener;

    InMemoryGraphTracker(
        DatabaseManagementService databaseManagementService,
        InMemoryGraphTrackerDatabaseEventListener inMemoryGraphTrackerDatabaseEventListener
    ) {
        this.databaseManagementService = databaseManagementService;
        this.inMemoryGraphTrackerDatabaseEventListener = inMemoryGraphTrackerDatabaseEventListener;
    }

    static InMemoryGraphTracker create(
        DatabaseManagementService databaseManagementService,
        GraphStoreCatalogService graphStoreCatalogService
    ) {
        var inMemoryGraphTrackerDatabaseEventListener = new InMemoryGraphTrackerDatabaseEventListener(
            databaseManagementService,
            graphStoreCatalogService
        );

        return new InMemoryGraphTracker(
            databaseManagementService,
            inMemoryGraphTrackerDatabaseEventListener
        );
    }

    @Override
    public void init() throws Exception {
        databaseManagementService.registerDatabaseEventListener(inMemoryGraphTrackerDatabaseEventListener);
    }

    @Override
    public void shutdown() throws Exception {
        databaseManagementService.unregisterDatabaseEventListener(inMemoryGraphTrackerDatabaseEventListener);
    }
}
