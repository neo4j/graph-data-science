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

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.utils.progress.TaskStore;

/**
 * This is responsible for bridging the gap between the global GDS extension,
 * and the many database-level extensions.
 * It is where we keep shared state - a map of database name to task store.
 * This is created once, in the global extension, and lives for the lifetime of the system.
 */
public interface TaskStoreObserver {
    /**
     * When a database bootstraps, we create state for it
     */
    TaskStore onBootstrap(DatabaseId databaseId);

    /**
     * When a database shuts down, we remove residual state
     * NB: this is best-effort only, because for composite databases,
     * the database id from the extension is not the same as what gets resolved for use. Just too bad.
     */
    void onShutdown(DatabaseId databaseId);
}
