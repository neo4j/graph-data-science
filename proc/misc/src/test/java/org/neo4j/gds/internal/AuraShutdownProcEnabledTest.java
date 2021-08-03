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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.compat.GraphStoreExportSettings;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatCode;

class AuraShutdownProcEnabledTest extends BaseTest {

    @TempDir
    Path tempDir;

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(AuraMaintenanceSettings.maintenance_function_enabled, true);
        builder.setConfig(GraphStoreExportSettings.export_location_setting, tempDir);
        builder.addExtension(new AuraMaintenanceExtension());
    }

    @Test
    void shouldBeVisibleWithTheSettingEnabled() {
        assertThatCode(() -> runQuery("CALL gds.internal.shutdown()"))
            .doesNotThrowAnyException();
    }
}
