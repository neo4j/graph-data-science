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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.embeddings.graphsage.EmptyGraphSageTrainMetrics;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.internal.BackupAndRestore.BackupResult;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.gdl.GdlFactory;
import org.neo4j.graphalgo.gdl.ImmutableGraphCreateFromGdlConfig;
import org.neo4j.kernel.database.DatabaseIdFactory;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class BackupAndRestoreTest {

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void shouldCreateModelsFolderWhenNoModelsAreInBackup(@TempDir Path tempDir) {
        var gdlGraph = "CREATE" +
                       "  (a:Label1 {prop1: 42})" +
                       ", (b:Label1)" +
                       ", (c:Label2 {prop2: 1337})" +
                       ", (d:Label2 {prop2: 10})" +
                       ", (e:Label2)" +
                       ", (a)-[:REL1]->(b)" +
                       ", (c)-[:REL2]->(d)";
        var graphConfig = ImmutableGraphCreateFromGdlConfig
            .builder()
            .gdlGraph(gdlGraph)
            .graphName("first")
            .username("userA")
            .build();
        GraphStoreCatalog.set(graphConfig, GdlFactory.of(gdlGraph).build().graphStore());

        var backupPath = tempDir.resolve("backup-1337");
        var results = BackupAndRestore.backup(backupPath, new TestLog(), 42, AllocationTracker.empty());

        assertThat(results).hasSize(1)
            .element(0, InstanceOfAssertFactories.type(BackupResult.class))
            .returns("graph", BackupResult::type)
            .returns(true, BackupResult::done)
            .satisfies(result -> {
                assertThat(result.exportMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(result.path()).isNotNull()
                    .startsWith(backupPath.toString());
            });

        assertThat(backupPath)
            .isDirectoryContaining("glob:**/graphs")
            .isDirectoryContaining("glob:**/models");
    }

    @Test
    void shouldCreateGraphsFolderWhenNoGraphsAreInBackup(@TempDir Path tempDir) {
        var model = Model.of(
            "userA",
            "firstModel",
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction()),
            GraphSageTrainConfig.builder().modelName("firstModel").addFeatureProperty("foo").build(),
            EmptyGraphSageTrainMetrics.INSTANCE
        );
        ModelCatalog.set(model);

        var backupPath = tempDir.resolve("backup-42");
        var results = BackupAndRestore.backup(backupPath, new TestLog(), 42, AllocationTracker.empty());

        assertThat(results).hasSize(1)
            .element(0, InstanceOfAssertFactories.type(BackupResult.class))
            .returns("model", BackupResult::type)
            .returns(true, BackupResult::done)
            .satisfies(result -> {
                assertThat(result.exportMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(result.path()).isNotNull()
                    .startsWith(backupPath.toString());
            });

        assertThat(backupPath)
            .isDirectoryContaining("glob:**/graphs")
            .isDirectoryContaining("glob:**/models");
    }

    @ParameterizedTest
    @CsvSource({
        "backup-1af160e1-806e-48eb-b00a-004e6aa227aa, true, true",
        "backup-2af160e1-806e-48eb-b00a-004e6aa227aa, true, false",
        "backup-3af160e1-806e-48eb-b00a-004e6aa227aa, false, true",
    })
    void shouldRestoreFromBackupLocation(String backupName, boolean hasGraphs, boolean hasModels) {
        assertThat(GraphStoreCatalog.graphStoresCount()).isEqualTo(0);
        assertThat(ModelCatalog.getAllModels().count()).isEqualTo(0);

        var backupPath = importPath().resolve(backupName);
        BackupAndRestore.restore(backupPath, new TestLog());

        if (hasGraphs) {
            assertGraphsAreRestored();
        } else {
            assertThat(GraphStoreCatalog.graphStoresCount()).isEqualTo(0);
        }

        if (hasModels) {
            assertModelsAreRestored();
        } else {
            assertThat(ModelCatalog.getAllModels().count()).isEqualTo(0);
        }
    }

    @Test
    void shouldRestoreOnlyGraphsFromBackupLocation() {
        assertThat(GraphStoreCatalog.graphStoresCount()).isEqualTo(0);
        assertThat(ModelCatalog.getAllModels().count()).isEqualTo(0);

        var backupPath = importPath().resolve("backup-1af160e1-806e-48eb-b00a-004e6aa227aa").resolve("graphs");
        BackupAndRestore.restore(backupPath, new TestLog());

        assertGraphsAreRestored();
        assertThat(ModelCatalog.getAllModels().count()).isEqualTo(0);
    }

    private void assertGraphsAreRestored() {
        assertThat(GraphStoreCatalog.graphStoresCount()).isEqualTo(1);

        var databaseId = DatabaseIdFactory.from("neo4j", UUID.fromString("26ca2600-2f7a-4e99-acc1-9d976603698c"));
        var testGraphStore = GraphStoreCatalog.get("UserA", databaseId, "test-graph");
        assertThat(testGraphStore).isNotNull();

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

        assertGraphEquals(expectedGraph, testGraphStore.graphStore().getUnion());
    }

    private void assertModelsAreRestored() {
        var aliceModels = ModelCatalog.list("alice");
        var bobModels = ModelCatalog.list("bob");

        assertThat(aliceModels).hasSize(1);
        assertThat(aliceModels.stream().findFirst().get().name()).isEqualTo("modelAlice");

        assertThat(bobModels).hasSize(1);
        assertThat(bobModels.stream().findFirst().get().name()).isEqualTo("modelBob");
    }

    private Path importPath() {
        try {
            var uri = Objects
                .requireNonNull(getClass().getClassLoader().getResource("BackupAndRestoreTest"))
                .toURI();
            return Paths.get(uri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
