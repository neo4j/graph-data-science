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

import org.apache.commons.io.file.PathUtils;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.compat.GdsGraphDatabaseAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.UUID;
import java.util.stream.StreamSupport;

public final class DatasetManager {
    private static final Logger Log = LoggerFactory.getLogger(DatasetManager.class);

    private final Map<String, Dataset> datasets = new HashMap<>() {{
        put(EmptyDataset.NAME, EmptyDataset.INSTANCE);
        put(FakeLdbcDataset.NAME, FakeLdbcDataset.INSTANCE);
        put(Cora.ID, new Cora());
        put(TestCora.ID, new TestCora());
    }};

    private final Path workingDir;
    private final Map<String, String> neo4jConfig;
    private final DbCreator dbCreator;

    public DatasetManager(Path workingDir, DbCreator dbCreator) {
        this(workingDir, dbCreator, Map.of());
    }

    public DatasetManager(Path workingDir, DbCreator dbCreator, Map<String, String> neo4jConfig) {
        this.workingDir = workingDir;
        this.neo4jConfig = new HashMap<>(neo4jConfig);
        this.dbCreator = dbCreator;
    }

    public Map<String, Dataset> datasets() {
        return Collections.unmodifiableMap(datasets);
    }

    public void registerDataset(String datasetId, Dataset dataset) {
        datasets.put(datasetId, dataset);
    }

    public GdsGraphDatabaseAPI openDb(String datasetId) {
        Log.info("opening dataset {}", datasetId);

        if (!datasets.containsKey(datasetId)) {
            throw new RuntimeException("Unknown dataset name " + datasetId);
        }

        Path datasetDirectory = datasetsDirectory().resolve(datasetId);

        if (!hasDataset(datasetId)) {
            Log.info("preparing dataset {}", datasetId);
            try {
                this.datasets.get(datasetId).prepare(datasetDirectory, dbCreator);
            } catch (IOException e) {
                throw new RuntimeException("Failed to download dataset" + datasetId, e);
            }
        }

        try {
            Log.debug("dataset {} is ready", datasetId);
            Log.debug(EnvironmentReporting.directoryContents(datasetsDirectory()));
            Log.debug(EnvironmentReporting.diskUsage());
        } catch (IOException ignore) {
            // not ideal
        }

        String workingCopyId = datasetId + "-" + UUID.randomUUID();
        Path workingCopyDirectory = workingCopiesDirectory().resolve(workingCopyId);

        try {
            Files.createDirectories(workingCopyDirectory);
            PathUtils.copyDirectory(datasetDirectory, workingCopyDirectory);
            Log.info("working copy of {} ready in {}", datasetId, workingCopyDirectory);
            Log.debug(EnvironmentReporting.directoryContents(workingCopiesDirectory()));
            Log.debug(EnvironmentReporting.diskUsage());
        } catch (IOException e) {
            throw new RuntimeException("Could not create working copy", e);
        }

        Log.info("starting Neo4j on {}", workingCopyDirectory);
        return openDb(workingCopyDirectory);
    }

    public void closeDb(GdsGraphDatabaseAPI db) {
        Log.info("closing database");
        if (db != null) {
            Path dbDir = db.dbHome(workingDir);
            db.shutdown();
            try {
                Log.info("cleaning up database directory {}...", dbDir);
                PathUtils.deleteDirectory(dbDir);
                Log.info("cleanup successful");
                Log.debug(EnvironmentReporting.directoryContents(workingCopiesDirectory()));
                Log.debug(EnvironmentReporting.diskUsage());
            } catch (IOException e) {
                Log.error("cleanup unsuccessful");
                throw new RuntimeException("Could not delete working copy", e);
            }
        }
    }

    private GdsGraphDatabaseAPI openDb(Path dbLocation) {
        GdsGraphDatabaseAPI db = dbCreator.createEmbeddedDatabase(dbLocation, neo4jConfig);
        Runtime.getRuntime().addShutdownHook(new Thread(db::shutdown));
        return db;
    }

    private boolean hasDataset(String datasetId) {
        Log.info("looking for {} in {}", datasetId, datasetsDirectory());
        if (! Files.exists(datasetsDirectory())) return false;

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(datasetsDirectory())) {
            Spliterator<Path> spliterator = directoryStream.spliterator();
            return StreamSupport
                .stream(spliterator, false)
                .anyMatch(folder -> folder.toFile().getName().equals(datasetId));
        } catch (IOException e) {
            throw new RuntimeException("Could not list existing datasets", e);
        }
    }

    @NotNull
    private Path workingCopiesDirectory() {
        return workingDir.resolve("working-copies");
    }

    @NotNull
    private Path datasetsDirectory() {
        return workingDir.resolve("datasets");
    }
}
