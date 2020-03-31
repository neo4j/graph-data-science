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

import org.neo4j.graphalgo.compat.SettingsProxy;
import org.neo4j.graphalgo.core.concurrency.ConcurrencyControllerExtension;
import org.neo4j.graphalgo.compat.GraphDbApi;
import org.neo4j.graphalgo.core.utils.mem.GcListenerExtension;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.UUID;

public final class TestDatabaseCreator {

    public static GraphDbApi createTestDatabase() {
        return new GraphDbApi(createDefault());
    }

    public static GraphDbApi createTestDatabaseWithCustomLoadCsvRoot(String value) {
        return new GraphDbApi(createWithCustomLoadCsvRoot(value));
    }

    public static GraphDbApi createUnlimitedConcurrencyTestDatabase() {
        return new GraphDbApi(createUnlimited());
    }

    public static GraphDbApi createEmbeddedDatabase(File storeDir) {
        return new GraphDbApi(createEmbedded(storeDir));
    }

    private static GraphDatabaseAPI createDefault() {
        return (GraphDatabaseAPI) builder().newGraphDatabase();
    }

    private static GraphDatabaseAPI createWithCustomLoadCsvRoot(String value) {
        return (GraphDatabaseAPI) builder()
            .setConfig(GraphDatabaseSettings.load_csv_file_url_root, value)
            .newGraphDatabase();
    }

    private static GraphDatabaseAPI createUnlimited() {
        return (GraphDatabaseAPI) builder()
            .setConfig(SettingsProxy.unlimitedCores(), "true")
            .newGraphDatabase();
    }

    private static GraphDatabaseAPI createEmbedded(File storeDir) {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory()
            .addKernelExtension(new ConcurrencyControllerExtension())
            .newEmbeddedDatabase(storeDir);
    }

    private static GraphDatabaseBuilder builder() {
        return new TestGraphDatabaseFactory()
            .addKernelExtension(new ConcurrencyControllerExtension())
            .addKernelExtension(new GcListenerExtension())
            .newImpermanentDatabaseBuilder(new File(UUID.randomUUID().toString()))
            .setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*");
    }

    private TestDatabaseCreator() {}
}
