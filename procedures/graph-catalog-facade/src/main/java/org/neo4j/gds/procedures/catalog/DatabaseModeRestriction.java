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
package org.neo4j.gds.procedures.catalog;

import org.neo4j.gds.compat.DatabaseMode;
import org.neo4j.gds.compat.SettingProxy;
import org.neo4j.graphdb.GraphDatabaseService;

public class DatabaseModeRestriction {
    private final GraphDatabaseService databaseService;

    public DatabaseModeRestriction(GraphDatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    /**
     * @throws java.lang.IllegalStateException if you are running on a cluster
     */
    void ensureNotOnCluster() {
        var neo4jMode = SettingProxy.databaseMode(databaseService);

        if (neo4jMode == DatabaseMode.CORE || neo4jMode == DatabaseMode.READ_REPLICA)
            throw new IllegalStateException(
                "The requested operation is not available while running Neo4j Graph Data Science library on a Neo4j Cluster."
            );
    }
}
