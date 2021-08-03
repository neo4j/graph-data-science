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
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.model.StoredModel;
import org.neo4j.gds.model.storage.ModelToFileExporter;
import org.neo4j.gds.utils.StringFormatting;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.io.file.CsvGraphStoreImporter;
import org.neo4j.gds.core.utils.io.file.GraphStoreExporterUtil;
import org.neo4j.gds.core.utils.io.file.ImmutableCsvGraphStoreImporterConfig;
import org.neo4j.gds.core.utils.io.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.utils.CheckedRunnable;
import org.neo4j.gds.utils.CheckedSupplier;
import org.neo4j.gds.utils.ExceptionUtil;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.core.utils.io.GraphStoreExporter.DIRECTORY_IS_WRITABLE;
import static org.neo4j.gds.core.utils.io.file.CsvGraphStoreImporter.DIRECTORY_IS_READABLE;
import static org.neo4j.gds.utils.ExceptionUtil.function;

public final class BackupAndRestore {

    private static final String GRAPHS_DIR = "graphs";
    private static final String MODELS_DIR = "models";
    private static final String BACKUP_META_DATA_FILE = ".backupmetadata";

    @ValueClass
    interface BackupResult {

        String backupId();

        ZonedDateTime backupTime();

        String username();

        String type();

        boolean done();

        @Nullable String path();

        long exportMillis();
    }

    @ValueClass
    interface BackupConfig {

        Path backupsPath();

        long timeoutInSeconds();

        String taskName();

        AllocationTracker allocationTracker();

        Log log();

        Optional<String> providedBackupId();

        Optional<Path> providedBackupPath();

        @Value.Default
        default String backupNameTemplate() {
            return "backup-%s";
        }

        @Value.Default
        default int maxAllowedBackups() {
            return -1;
        }

        @Value.Default
        default Consumer<GraphStoreCatalog.GraphStoreWithUserNameAndConfig> graphOnSuccess() {
            return ignore -> {};
        }

        @Value.Default
        default Consumer<Model<?, ?>> modelOnSuccess() {
            return ignore -> {};
        }

        default String backupName(String backupId) {
            return formatWithLocale(backupNameTemplate(), backupId);
        }

        default Path backupPath(String backupId) {
            return providedBackupPath().orElseGet(() -> backupsPath().resolve(backupName(backupId)));
        }
    }

    static List<BackupResult> backup(BackupConfig backupConfig) {
        var metadataBuilder = ImmutableBackupMetadata.builder();
        backupConfig.providedBackupId().ifPresent(metadataBuilder::backupId);
        var metadata = metadataBuilder.build();

        try {
            var createBackup = canCreateBackup(backupConfig.backupsPath(), backupConfig.maxAllowedBackups(), metadata);
            return applyBackup(
                createBackup,
                () -> doBackup(
                    backupConfig.taskName(),
                    metadata,
                    backupConfig.backupPath(metadata.backupId()),
                    backupConfig.graphOnSuccess(),
                    backupConfig.modelOnSuccess(),
                    backupConfig.timeoutInSeconds(),
                    backupConfig.log(),
                    backupConfig.allocationTracker()
                ),
                List::of
            );
        } catch (IOException e) {
            backupConfig.log().error(
                formatWithLocale(
                    "Failed to create backup '%s', aborting %s.",
                    metadata.backupId(),
                    StringFormatting.toLowerCaseWithLocale(backupConfig.taskName())
                ),
                e
            );
            return List.of();
        }
    }

    private static List<BackupResult> doBackup(
        String taskName,
        BackupMetadata metadata,
        Path backupPath,
        Consumer<GraphStoreCatalog.GraphStoreWithUserNameAndConfig> graphOnSuccess,
        Consumer<Model<?, ?>> modelOnSuccess,
        long timeoutInSeconds,
        Log log,
        AllocationTracker allocationTracker
    ) {
        log.info("Preparing for %s", StringFormatting.toLowerCaseWithLocale(taskName));

        DIRECTORY_IS_WRITABLE.validate(backupPath);

        try {
            writeBackupMetadata(backupPath, metadata);
        } catch (IOException e) {
            log.error(
                formatWithLocale(
                    "Failed to write metadata file '%s', aborting %s.",
                    backupPath.resolve(BACKUP_META_DATA_FILE),
                    StringFormatting.toLowerCaseWithLocale(taskName)
                ),
                e
            );
            return List.of();
        }

        var factory = new BackupResultFactory(metadata);
        var result = new ArrayList<BackupResult>();

        var timer = ProgressTimer.start();
        try (timer) {
            result.addAll(backupGraphStores(
                factory,
                backupPath,
                graphOnSuccess,
                log,
                allocationTracker
            ));
            result.addAll(backupModels(
                factory,
                backupPath,
                modelOnSuccess,
                log
            ));

            // We have the contract that both graphs and models folder exist, even if they do not contain any data
            result.stream().flatMap(backupResult -> {
                var userPath = backupPath.resolve(backupResult.username());
                return Stream.of(userPath.resolve(GRAPHS_DIR), userPath.resolve(MODELS_DIR));
            }).distinct().forEach(pathToCreate -> {
                try {
                    Files.createDirectories(pathToCreate);
                } catch (IOException e) {
                    log.error(
                        formatWithLocale(
                            "Failed to create %s directory '%s'",
                            StringFormatting.toLowerCaseWithLocale(taskName),
                            pathToCreate
                        ),
                        e
                    );
                }
            });
        }

        var elapsedTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(timer.getDuration());
        if (elapsedTimeInSeconds > timeoutInSeconds) {
            log.warn(
                "%s took too long, the actual time of %d seconds is greater than the provided timeout of %d seconds",
                taskName,
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        } else {
            log.info(
                "%s finished within the given timeout, it took %d seconds and the provided timeout was %d seconds.",
                taskName,
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        }
        return result;
    }

    static void restore(Path restorePath, Path backupPath, int maxAllowedBackups, Log log) {
        var successfulRestorePaths = subdirectories(restorePath, log)
            .flatMap(userRestorePath -> {
                try {
                    restoreForUser(userRestorePath, log);
                    return Stream.of(userRestorePath);
                } catch (Exception e) {
                    log.error(formatWithLocale("Restore failed for user path '%s'", userRestorePath), e);
                    return Stream.empty();
                }
            })
            .collect(Collectors.toList());

        if (!successfulRestorePaths.isEmpty()) {
            try {
                createNewBackupFromRestore(backupPath, restorePath, maxAllowedBackups, successfulRestorePaths);
            } catch (IOException e) {
                log.error(
                    formatWithLocale(
                        "Could not move the restored files to the backup folder '%s'",
                        backupPath
                    ),
                    e
                );
            }
        }
    }

    private static void restoreForUser(Path userRestorePath, Log log) {
        var graphsPath = userRestorePath.resolve(GRAPHS_DIR);
        var modelsPath = userRestorePath.resolve(MODELS_DIR);
        DIRECTORY_IS_READABLE.validate(graphsPath);
        DIRECTORY_IS_READABLE.validate(modelsPath);

        restoreGraphs(graphsPath, log);
        restoreModels(modelsPath, log);
    }

    private static void createNewBackupFromRestore(
        Path backupPath,
        Path restorePath,
        int maxAllowedBackups,
        Iterable<Path> userPaths
    )
    throws IOException {
        var metadataFile = restorePath.resolve(BACKUP_META_DATA_FILE);
        var metadata = readBackupMetadata(metadataFile);

        var createBackup = canCreateBackup(backupPath, maxAllowedBackups, metadata);
        applyBackup(createBackup, () -> {
            var backupName = formatWithLocale("backup-%s-restored", metadata.backupId());
            var backupRoot = backupPath.resolve(backupName);
            Files.createDirectories(backupRoot);
            Files.move(metadataFile, backupRoot.resolve(BACKUP_META_DATA_FILE));
            for (Path userPath : userPaths) {
                Files.move(userPath, backupRoot.resolve(userPath.getFileName()));
            }
        });
    }

    private static CreateBackup canCreateBackup(Path backupsPath, int maxAllowedBackups, BackupMetadata metadata)
    throws IOException {
        var builder = ImmutableCreateBackup.builder().shouldBackup(true);

        if (!Files.exists(backupsPath)) {
            return builder.build();
        }

        var existingBackups = Files.list(backupsPath)
            .filter(BackupAndRestore::isBackupDirectory)
            .map(function(path -> ImmutableBackup.of(readBackupMetadata(path.resolve(BACKUP_META_DATA_FILE)), path)))
            .collect(Collectors.toList());

        if (maxAllowedBackups >= 0 && existingBackups.size() >= maxAllowedBackups) {
            var oldestBackup = existingBackups.stream()
                .filter(entry -> entry.backupTime().isBefore(metadata.backupTime()))
                .min(Comparator.comparing(Backup::backupTime));

            oldestBackup.ifPresentOrElse(
                builder::removeBefore,
                () -> builder.shouldBackup(false)
            );
        }

        return builder.build();
    }

    private static <E extends IOException> void applyBackup(CreateBackup createBackup, CheckedRunnable<E> backupAction)
    throws IOException {
        applyBackup(createBackup, () -> {
            backupAction.run();
            return null;
        }, () -> null);
    }

    private static <T, E extends IOException> T applyBackup(
        CreateBackup createBackup,
        CheckedSupplier<T, E> backupAction,
        Supplier<T> empty
    )
    throws IOException {
        if (createBackup.shouldBackup()) {
            var removeBefore = createBackup.removeBefore();
            if (removeBefore.isPresent()) {
                PathUtils.deleteDirectory(removeBefore.get().location());
            }

            return backupAction.checkedGet();
        }
        return empty.get();
    }

    private static boolean isBackupDirectory(Path path) {
        return Files.isDirectory(path) && Files.isReadable(path) && Files.exists(path.resolve(BACKUP_META_DATA_FILE));
    }

    private static List<BackupResult> backupGraphStores(
        BackupResultFactory factory,
        Path backupRoot,
        Consumer<GraphStoreCatalog.GraphStoreWithUserNameAndConfig> onSuccess,
        Log log,
        AllocationTracker allocationTracker
    ) {
        return GraphStoreCatalog.getAllGraphStores()
            .flatMap(store -> {
                var username = store.userName();
                try {
                    var config = ImmutableGraphStoreToFileExporterConfig
                        .builder()
                        .includeMetaData(true)
                        .exportName(store.config().graphName())
                        .username(username)
                        .build();

                    var backupDir = backupRoot.resolve(username).resolve(GRAPHS_DIR);
                    var backupPath = GraphStoreExporterUtil.exportPath(backupDir, config);

                    var timer = ProgressTimer.start();
                    GraphStoreExporterUtil.export(
                        store.graphStore(),
                        backupPath,
                        config,
                        Optional.empty(),
                        log,
                        allocationTracker
                    );
                    timer.stop();

                    onSuccess.accept(store);

                    return Stream.of(factory.successfulGraph(username, backupPath, timer));
                } catch (Exception e) {
                    log.error(
                        formatWithLocale(
                            "Persisting graph '%s' for user '%s' failed",
                            store.config().graphName(),
                            username
                        ),
                        e
                    );
                    return Stream.of(factory.failedGraph(username));
                }
            }).collect(Collectors.toList());
    }

    private static List<BackupResult> backupModels(
        BackupResultFactory factory,
        Path backupRoot,
        Consumer<Model<?, ?>> onSuccess,
        Log log
    ) {
        return ModelCatalog.getAllModels().flatMap(model -> {
            var username = model.creator();
            try {
                var backupDir = backupRoot.resolve(username).resolve(MODELS_DIR);
                var modelPath = backupDir.resolve(UUID.randomUUID().toString());
                DIRECTORY_IS_WRITABLE.validate(modelPath);

                var timer = ProgressTimer.start();
                ModelToFileExporter.toFile(modelPath, model);
                timer.stop();

                onSuccess.accept(model);

                return Stream.of(factory.successfulModel(username, modelPath, timer));
            } catch (Exception e) {
                log.error(
                    formatWithLocale(
                        "Persisting model '%s' for user '%s' failed",
                        model.name(),
                        username
                    ),
                    e
                );
                return Stream.of(factory.failedModel(username));
            }
        }).collect(Collectors.toList());
    }

    private static void restoreGraphs(Path storePath, Log log) {
        subdirectories(storePath, log).forEach(path -> restoreGraph(path, log));
    }

    private static void restoreGraph(Path path, Log log) {
        var config = ImmutableCsvGraphStoreImporterConfig.builder().build();
        var graphStoreImporter = CsvGraphStoreImporter.create(config, path, log);

        graphStoreImporter.run(AllocationTracker.empty());

        var graphStore = graphStoreImporter.userGraphStore();

        var graphName = path.getFileName().toString();
        var createConfig = GraphCreateFromStoreConfig.emptyWithName(
            graphStore.userName(),
            graphName
        );
        GraphStoreCatalog.set(createConfig, graphStore.graphStore());
    }

    private static void restoreModels(Path storePath, Log log) {
        subdirectories(storePath, log).forEach(path -> restoreModel(path, log));
    }

    private static void restoreModel(Path storedModelPath, Log log) {
        try {
            var model = new StoredModel(storedModelPath);
            if (ModelCatalog.exists(model.creator(), model.name())) {
                log.error(
                    "Cannot open stored model %s for user %s from %s. A model with the same name already exists for that user.",
                    model.name(),
                    model.creator(),
                    storedModelPath
                );
            } else {
                ModelCatalog.set(model);
            }
        } catch (IOException e) {
            log.error(
                formatWithLocale(
                    "Could not load model stored at %s",
                    storedModelPath
                ), e
            );
        }
    }

    private static Stream<Path> subdirectories(Path path, Log log) {
        try {
            return Files.list(path)
                .filter(not(dir -> ExceptionUtil.supply(() -> Files.isHidden(dir))))
                .peek(DIRECTORY_IS_READABLE::validate);
        } catch (IOException e) {
            log.error(
                formatWithLocale("Could not traverse subdirectories of '%s'", path),
                e
            );
            return Stream.empty();
        }
    }

    private BackupAndRestore() {}

    private static void writeBackupMetadata(Path backupRoot, BackupMetadata metadata) throws IOException {
        Files.write(
            backupRoot.resolve(BACKUP_META_DATA_FILE),
            List.of(
                formatWithLocale("Backup-Time: %d", metadata.backupTime().toInstant().toEpochMilli()),
                formatWithLocale("Backup-Id: %s", metadata.backupId())
            ),
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE_NEW
        );
    }

    private static BackupMetadata readBackupMetadata(Path metadataFile) throws IOException {
        var lines = Files.lines(metadataFile, StandardCharsets.UTF_8);
        try (lines) {
            return lines.reduce(ImmutableBackupMetadata.builder(), (b, line) -> {
                var parts = line.split(": ", 2);
                switch (parts[0]) {
                    case "Backup-Time":
                        var epochMillis = Long.parseLong(parts[1], 10);
                        var instant = Instant.ofEpochMilli(epochMillis);
                        var backupTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
                        return b.backupTime(backupTime);

                    case "Backup-Id":
                        return b.backupId(parts[1]);

                    default:
                        throw new IllegalStateException(formatWithLocale(
                            "Unexpected identifier '%s' in the backup metadata at '%s'",
                            parts[0],
                            metadataFile
                        ));
                }
            }, (b1, b2) -> b1).build();
        }
    }

    @ValueClass
    interface BackupMetadata {
        @Value.Default
        default String backupId() {
            return UUID.randomUUID().toString();
        }

        @Value.Default
        default ZonedDateTime backupTime() {
            return ZonedDateTime.now(Clock.systemUTC());
        }
    }

    @ValueClass
    interface Backup {
        BackupMetadata metadata();

        Path location();

        default ZonedDateTime backupTime() {
            return metadata().backupTime();
        }
    }

    @ValueClass
    interface CreateBackup {
        /**
         * @return false if this backup is not newer than any of the backups found in the backup location
         *     and the limit on the maximum number of backups is reached.
         *     true if this backup is later than existing backups or there is
         *     no limit on the maximum number of backups.
         */
        boolean shouldBackup();

        /**
         * If this returns _some_ backup, that one needs to be deleted before continuing with the backup.
         * Only relevant if {@link #shouldBackup()} returns true.
         */
        Optional<Backup> removeBefore();
    }

    private static class BackupResultFactory {
        private final BackupMetadata metadata;

        BackupResultFactory(BackupMetadata metadata) {
            this.metadata = metadata;
        }

        private BackupResult successfulGraph(String username, Path path, ProgressTimer timer) {
            return ImmutableBackupResult.of(
                metadata.backupId(),
                metadata.backupTime(),
                username,
                "graph",
                true,
                path.toString(),
                timer.getDuration()
            );
        }

        private BackupResult failedGraph(String username) {
            return ImmutableBackupResult.of(
                metadata.backupId(),
                metadata.backupTime(),
                username,
                "graph",
                false,
                null,
                0
            );
        }

        private BackupResult successfulModel(String username, Path path, ProgressTimer timer) {
            return ImmutableBackupResult.of(
                metadata.backupId(),
                metadata.backupTime(),
                username,
                "model",
                true,
                path.toString(),
                timer.getDuration()
            );
        }

        private BackupResult failedModel(String username) {
            return ImmutableBackupResult.of(
                metadata.backupId(),
                metadata.backupTime(),
                username,
                "model",
                false,
                null,
                0
            );
        }
    }
}
