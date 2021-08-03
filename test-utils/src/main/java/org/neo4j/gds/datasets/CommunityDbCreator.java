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
package org.neo4j.gds.datasets;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.gds.compat.GdsGraphDatabaseAPI;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.Settings;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public final class CommunityDbCreator implements DbCreator {

    private static final CommunityDbCreator INSTANCE = new CommunityDbCreator();

    public static CommunityDbCreator getInstance() {
        return INSTANCE;
    }

    public GdsGraphDatabaseAPI createEmbeddedDatabase(Path storeDir) {
        DatabaseManagementService dbms = builder(storeDir).build();
        return Neo4jProxy.newDb(dbms);
    }

    public GdsGraphDatabaseAPI createEmbeddedDatabase(Path storeDir, Map<String, String> config) {
        DatabaseManagementService dbms = builder(storeDir)
            .setConfigRaw(config)
            .build();
        return Neo4jProxy.newDb(dbms);
    }

    private static DatabaseManagementServiceBuilder builder(Path storeDir) {
        return new DatabaseManagementServiceBuilder(storeDir.toFile())
            .setConfig(Settings.procedureUnrestricted(), List.of("gds.*"))
            .setConfig(Settings.udc(), false)
            .setConfig(Settings.boltEnabled(), false)
            .setConfig(Settings.httpEnabled(), false)
            .setConfig(Settings.httpsEnabled(), false);
    }

    private CommunityDbCreator() {}
}
