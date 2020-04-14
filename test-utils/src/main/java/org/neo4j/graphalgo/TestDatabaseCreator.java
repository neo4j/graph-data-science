/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphalgo.compat.GraphDbApi;
import org.neo4j.graphalgo.compat.SettingsProxy;
import org.neo4j.graphalgo.core.concurrency.ConcurrencyControllerExtension;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;

import static java.util.Collections.singletonList;

public final class TestDatabaseCreator {

    public static GraphDbApi createTestDatabase() {
        return new GraphDbApi(createDefault());
    }

    public static GraphDbApi createUnlimitedConcurrencyTestDatabase() {
        return new GraphDbApi(createUnlimited());
    }

    public static GraphDbApi createEmbeddedDatabase(File storeDir) {
        return new GraphDbApi(createEmbedded(storeDir));
    }

    private static DatabaseManagementService createDefault() {
        return builder().build();
    }

    private static DatabaseManagementService createUnlimited() {
        return builder()
            .setConfig(SettingsProxy.unlimitedCores(), true)
            .build();
    }

    private static DatabaseManagementService createEmbedded(File storeDir) {
        return anyDbBuilder(storeDir)
            .setConfig(GraphDatabaseSettings.fail_on_missing_files, false)
            .build();
    }

    private static TestDatabaseManagementServiceBuilder builder() {
        return anyDbBuilder(null).impermanent();
    }

    private static TestDatabaseManagementServiceBuilder anyDbBuilder(File testDir) {
        return new TestDatabaseManagementServiceBuilder(testDir)
            .addExtension(new ConcurrencyControllerExtension())
            .setConfig(GraphDatabaseSettings.procedure_unrestricted, singletonList("gds.*"));
    }

    private TestDatabaseCreator() {}
}
