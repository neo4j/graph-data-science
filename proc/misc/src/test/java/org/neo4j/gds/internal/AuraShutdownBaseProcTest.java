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
package org.neo4j.gds.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.compat.GraphStoreExportSettings;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.nio.file.Path;

public abstract class AuraShutdownBaseProcTest extends BaseProcTest {

    @TempDir
    Path tempDir;

    TestLog testLog;

    @BeforeEach
    void setup() throws Exception {
        AuraTestSupport.setupGraphsAndModels(db);
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
        ModelCatalog.removeAllLoadedModels();
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        testLog = new TestLog();
        builder.setUserLogProvider(new LogProvider() {
            @Override
            public Log getLog(Class<?> loggingClass) {
                return testLog;
            }

            @Override
            public Log getLog(String name) {
                return testLog;
            }
        });
        builder.setConfig(GraphStoreExportSettings.export_location_setting, tempDir);
    }
}
