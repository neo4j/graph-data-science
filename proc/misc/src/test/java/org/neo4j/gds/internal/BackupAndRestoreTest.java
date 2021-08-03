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
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.embeddings.graphsage.EmptyGraphSageTrainMetrics;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.internal.BackupAndRestore.BackupResult;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.gdl.ImmutableGraphCreateFromGdlConfig;
import org.neo4j.kernel.database.DatabaseIdFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

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
            .username("alice")
            .build();
        GraphStoreCatalog.set(graphConfig, GdlFactory.of(gdlGraph).build().graphStore());

        var config = ImmutableBackupConfig
            .builder()
            .backupsPath(tempDir)
            .providedBackupId("1337")
            .timeoutInSeconds(42)
            .log(new TestLog())
            .allocationTracker(AllocationTracker.empty())
            .taskName("Backup")
            .build();

        var results = BackupAndRestore.backup(config);

        var backupPath = tempDir.resolve("backup-1337");

        assertThat(results).hasSize(1)
            .element(0, InstanceOfAssertFactories.type(BackupResult.class))
            .returns("1337", BackupResult::backupId)
            .returns("graph", BackupResult::type)
            .returns(true, BackupResult::done)
            .satisfies(result -> {
                assertThat(result.exportMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(result.path()).isNotNull().startsWith(backupPath.toString());
            });

        assertThat(backupPath)
            .isDirectoryRecursivelyContaining("glob:**/alice/graphs")
            .isDirectoryRecursivelyContaining("glob:**/alice/models");

        assertThat(backupPath.resolve(".backupmetadata"))
            .isNotEmptyFile()
            .usingCharset(StandardCharsets.UTF_8)
            .hasContent(formatWithLocale(
                "Backup-Time: %d%nBackup-Id: 1337%n",
                results.get(0).backupTime().toInstant().toEpochMilli()
            ));
    }

    @Test
    void shouldCreateGraphsFolderWhenNoGraphsAreInBackup(@TempDir Path tempDir) {
        var model = Model.of(
            "alice",
            "firstModel",
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction()),
            GraphSageTrainConfig.builder().modelName("firstModel").addFeatureProperty("foo").build(),
            EmptyGraphSageTrainMetrics.INSTANCE
        );
        ModelCatalog.set(model);

        var config = ImmutableBackupConfig
            .builder()
            .backupsPath(tempDir)
            .providedBackupId("42")
            .timeoutInSeconds(42)
            .log(new TestLog())
            .allocationTracker(AllocationTracker.empty())
            .taskName("Backup")
            .build();

        var results = BackupAndRestore.backup(config);

        var backupPath = tempDir.resolve("backup-42");

        assertThat(results).hasSize(1)
            .element(0, InstanceOfAssertFactories.type(BackupResult.class))
            .returns("42", BackupResult::backupId)
            .returns("model", BackupResult::type)
            .returns(true, BackupResult::done)
            .satisfies(result -> {
                assertThat(result.exportMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(result.path()).isNotNull()
                    .startsWith(backupPath.toString());
            });

        assertThat(backupPath)
            .isDirectoryRecursivelyContaining("glob:**/alice/graphs")
            .isDirectoryRecursivelyContaining("glob:**/alice/models");

        assertThat(backupPath.resolve(".backupmetadata"))
            .isNotEmptyFile()
            .usingCharset(StandardCharsets.UTF_8)
            .hasContent(formatWithLocale(
                "Backup-Time: %d%nBackup-Id: 42%n",
                results.get(0).backupTime().toInstant().toEpochMilli()
            ));
    }

    @ParameterizedTest
    @CsvSource({
        "backup-1af160e1-806e-48eb-b00a-004e6aa227aa, true, true",
        "backup-2af160e1-806e-48eb-b00a-004e6aa227aa, true, false",
        "backup-3af160e1-806e-48eb-b00a-004e6aa227aa, false, true",
    })
    void shouldRestoreFromBackupLocation(
        String backupName,
        boolean hasGraphs,
        boolean hasModels,
        @TempDir Path tempDir
    ) throws IOException {
        var restorePath = importPath(tempDir.resolve("shutdown"), backupName);
        var backupPath = tempDir.resolve("backup");
        Files.createDirectories(backupPath);

        assertThat(GraphStoreCatalog.graphStoresCount()).isEqualTo(0);
        assertThat(ModelCatalog.getAllModels().count()).isEqualTo(0);

        BackupAndRestore.restore(restorePath, backupPath, -1, new TestLog());

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

        assertThat(restorePath)
            .exists()
            .isEmptyDirectory();

        var backupId = backupName.split("-", 2)[1];
        var backupLocation = backupPath.resolve(formatWithLocale("backup-%s-restored", backupId));

        if (hasGraphs) {
            assertThat(backupLocation).isDirectoryRecursivelyContaining("glob:**/alice/graphs/**");
            assertThat(backupLocation).isDirectoryRecursivelyContaining("glob:**/bob/graphs/**");
        }
        if (hasModels) {
            assertThat(backupLocation).isDirectoryRecursivelyContaining("glob:**/alice/models/**");
            assertThat(backupLocation).isDirectoryRecursivelyContaining("glob:**/bob/models/**");
        }

        var backupTime = LocalDateTime
            .of(2021, 8, 2, 13, 37, 42, (int) TimeUnit.MILLISECONDS.toNanos(84))
            .toInstant(ZoneOffset.UTC)
            .toEpochMilli();

        assertThat(backupLocation.resolve(".backupmetadata"))
            .isNotEmptyFile()
            .usingCharset(StandardCharsets.UTF_8)
            .hasContent(formatWithLocale(
                "Backup-Time: %d%nBackup-Id: %s%n",
                backupTime,
                backupId
            ));
    }

    @Test
    void shouldBackupIfPermitted(@TempDir Path tempDir) throws IOException {
        testBackupLimit(
            tempDir,
            // same time
            time -> time,
            1,
            "backup-42", "backup-43", "backup-44", "backup-1337"
        );
    }

    @Test
    void shouldRemoveOlderBackups(@TempDir Path tempDir) throws IOException {
        testBackupLimit(
            tempDir,
            // some time in the past
            time -> time.minus(1, ChronoUnit.YEARS),
            "backup-43", "backup-44", "backup-1337"
        );
    }

    @Test
    void shouldNotRemoveNewerBackups(@TempDir Path tempDir) throws IOException {
        testBackupLimit(
            tempDir,
            // some time in the future
            time -> time.plus(1, ChronoUnit.YEARS),
            "backup-42", "backup-43", "backup-44"
        );
    }


    void testBackupLimit(
        Path tempDir,
        UnaryOperator<LocalDateTime> travelInTime,
        String... expectedBackups
    ) throws IOException {
        testBackupLimit(tempDir, travelInTime, 0, expectedBackups);
    }

    void testBackupLimit(
        Path tempDir,
        UnaryOperator<LocalDateTime> travelInTime,
        int additionalAllowedBackups,
        String... expectedBackups
    ) throws IOException {
        // use the same time as base so that we don't run into issues
        // when the system clock changes between calls to now
        var now = LocalDateTime.now();
        int maxAllowedBackups = 3;
        for (int i = 0; i < maxAllowedBackups; i++) {
            // just create backup metadata without any actual backup files
            var backupId = 42 + i;
            var backupTime = travelInTime
                .apply(now)
                // spread the times by _i_ seconds to make them different
                // and put them into a defined order
                .plus(i, ChronoUnit.SECONDS)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();

            var backupDir = tempDir.resolve(formatWithLocale("backup-%d", backupId));
            Files.createDirectories(backupDir);

            Files.writeString(
                backupDir.resolve(".backupmetadata"),
                formatWithLocale("Backup-Time: %d%nBackup-Id: %d%n", backupTime, backupId)
            );
        }

        var config = ImmutableBackupConfig
            .builder()
            .maxAllowedBackups(maxAllowedBackups + additionalAllowedBackups)
            .backupsPath(tempDir)
            .providedBackupId("1337")
            .timeoutInSeconds(42)
            .log(new TestLog())
            .allocationTracker(AllocationTracker.empty())
            .taskName("Backup")
            .build();


        BackupAndRestore.backup(config);

        var remainingBackups = Files
            .list(tempDir)
            .map(path -> tempDir.relativize(path).toString())
            .collect(Collectors.toList());

        assertThat(remainingBackups).containsExactlyInAnyOrder(expectedBackups);
    }

    private void assertGraphsAreRestored() {
        assertThat(GraphStoreCatalog.graphStoresCount()).isEqualTo(2);

        var databaseId = DatabaseIdFactory.from("neo4j", UUID.fromString("26ca2600-2f7a-4e99-acc1-9d976603698c"));

        var aliceGraphStore = GraphStoreCatalog.get("alice", databaseId, "test-graph-1");
        assertThat(aliceGraphStore).isNotNull();

        var aliceGraph = TestSupport.fromGdl(
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
        assertGraphEquals(aliceGraph, aliceGraphStore.graphStore().getUnion());

        var bobGraphStore = GraphStoreCatalog.get("bob", databaseId, "test-graph-2");
        assertThat(bobGraphStore).isNotNull();

        var bobGraph = TestSupport.fromGdl("(n0:C)");
        assertGraphEquals(bobGraph, bobGraphStore.graphStore().getUnion());
    }

    private void assertModelsAreRestored() {
        var aliceModels = ModelCatalog.list("alice");
        var bobModels = ModelCatalog.list("bob");

        assertThat(aliceModels).hasSize(1);
        assertThat(aliceModels.stream().findFirst().get().name()).isEqualTo("modelAlice");

        assertThat(bobModels).hasSize(1);
        assertThat(bobModels.stream().findFirst().get().name()).isEqualTo("modelBob");
    }

    private Path importPath(Path tempDir, String... subFolders) {
        try {
            var uri = Objects
                .requireNonNull(getClass().getClassLoader().getResource("BackupAndRestoreTest"))
                .toURI();
            var resourceDirectory = Arrays
                .stream(subFolders)
                .reduce(Paths.get(uri), Path::resolve, (p1, p2) -> p1);
            PathUtils.copyDirectory(resourceDirectory, tempDir);
            return tempDir;
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
