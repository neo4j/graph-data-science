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
import org.neo4j.configuration.Config;
import org.neo4j.gds.AuraGraphRestorer;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.io.file.GraphStoreExporterUtil;
import org.neo4j.graphalgo.core.utils.io.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
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

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        prepareImportDir();
        super.configuration(builder);
        builder
            .setConfig(GraphStoreExportSettings.export_location_setting, importDir)
            .setConfig(AuraMaintenanceSettings.maintenance_function_enabled, true)
            .removeExtensions(ext -> ext instanceof AuraMaintenanceExtension)
            .addExtension(new AuraMaintenanceExtension(true));
    }

    @Test
    void shouldLoadOnlyAutoloadGraphStores() {
        assertThat(GraphStoreCatalog.graphStoresCount()).isEqualTo(1);

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

        var testGraphStore = GraphStoreCatalog.get("UserA", db.databaseId(), "test-graph");
        assertThat(testGraphStore).isNotNull();
        assertGraphEquals(expectedGraph, testGraphStore.graphStore().getUnion());
    }

    @Test
    void shouldRemoveImportedCsvFiles() throws IOException {
        var log = new TestLog();
        var importPath = importDir;
        var addedTestGraph = "test-graph2";

        // export an additional graph
        var graphStore = TestSupport.graphStoreFromGDL("(), ()-[:TYPE]->()");
        var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(db, Config.class);

        var exportConfig = ImmutableGraphStoreToFileExporterConfig.builder()
            .autoload(true)
            .includeMetaData(true)
            .exportName(addedTestGraph)
            .username("UserA")
            .build();
        GraphStoreExporterUtil.runGraphStoreExportToCsv(
            graphStore,
            neo4jConfig,
            exportConfig,
            log,
            AllocationTracker.empty()
        );

        // do an aura-like restore operation
        AuraGraphRestorer.restore(importPath, log);

        // the graphs should have been loaded
        assertThat(GraphStoreCatalog.graphStoresCount()).isEqualTo(2);

        // and the files should have been removed
        assertThat(importPath.resolve(addedTestGraph)).doesNotExist();
    }

    private void prepareImportDir() {
        try {
            var uri = Objects
                .requireNonNull(getClass().getClassLoader().getResource("AuraMaintenanceExtensionTest"))
                .toURI();
            var resourceDirectory = Paths.get(uri);
            PathUtils.copyDirectory(resourceDirectory, importDir);
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
