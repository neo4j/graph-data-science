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

import org.apache.commons.io.file.PathUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class AuraMaintenanceExtensionTest extends BaseTest {

    @TempDir
    Path importDir;

    private Path exportLocation;

    private Path backupLocation;

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
        ModelCatalog.removeAllLoadedModels();
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        exportLocation = importDir.resolve("export");
        backupLocation = importDir.resolve("backup");

        prepareImportDir(exportLocation);
        super.configuration(builder);

        builder
            .setConfig(GraphStoreExportSettings.export_location_setting, exportLocation)
            .setConfig(GraphStoreExportSettings.backup_location_setting, backupLocation)
            .setConfig(AuraMaintenanceSettings.maintenance_function_enabled, true)
            .removeExtensions(ext -> ext instanceof AuraMaintenanceExtension)
            .addExtension(new AuraMaintenanceExtension(true));
    }

    @Test
    void shouldLoadOnlyAutoloadGraphStores() {
        assertThat(GraphStoreCatalog.graphStoresCount()).isEqualTo(2);

        var expectedGraph = TestSupport.fromGdl(
            "  (n0:A {prop1: 21})" +
            ", (n1:A {prop1: 42})" +
            ", (n2:A {prop1: 23})" +
            ", (n3:A {prop1: 24})" +
            ", (:A { prop1: 25})" +
            ", (:B)" +
            ", (:B)" +
            ", (:B)" +
            ", (:B)" +
            ", (:B)" +
            ", (n0)-[:REL {weight: 1.5, height: 2.2}]->(n1)-[:REL {weight: 4.0, height: 2.3}]->(n2)-[:REL {weight: 4.2, height: 2.4}]->(n3)" +
            ", (n1)-[:REL1]->(n2)-[:REL1]->(n3)"
        );

        var testGraphStore = GraphStoreCatalog.get("UserA", db.databaseId(), "test-graph-1");
        assertThat(testGraphStore).isNotNull();
        assertGraphEquals(expectedGraph, testGraphStore.graphStore().getUnion());

        assertThat(exportLocation).isEmptyDirectory();
        assertThat(backupLocation).isDirectoryContaining("glob:**/backup-*-restored");
    }

    @Test
    void shouldLoadModelsIntoModelCatalog() {
        assertThat(ModelCatalog.getAllModels().count()).isEqualTo(2);

        var aliceModels = ModelCatalog.list("alice");
        var bobModels = ModelCatalog.list("bob");

        assertThat(aliceModels).hasSize(1);
        assertThat(aliceModels.stream().findFirst().get().name()).isEqualTo("modelAlice");

        assertThat(bobModels).hasSize(1);
        assertThat(bobModels.stream().findFirst().get().name()).isEqualTo("modelBob");

        assertThat(exportLocation).isEmptyDirectory();
        assertThat(backupLocation).isDirectoryContaining("glob:**/backup-*-restored");
    }

    private void prepareImportDir(Path exportLocation) {
        try {
            var uri = Objects
                .requireNonNull(getClass().getClassLoader().getResource("AuraMaintenanceExtensionTest"))
                .toURI();
            var resourceDirectory = Paths.get(uri);
            PathUtils.copyDirectory(resourceDirectory, exportLocation);
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
