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
package org.neo4j.graphalgo.datasets;

import org.apache.commons.io.file.PathUtils;
import org.neo4j.graphalgo.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphalgo.core.GdsEdition;

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
        if (!datasets.containsKey(datasetId)) {
            throw new RuntimeException("Unknown dataset name " + datasetId);
        }

        Path datasetDir = workingDir.resolve(datasetId);
        if (!hasDataset(datasetId)) {
            try {
                datasets.get(datasetId).generate(datasetDir, dbCreator);
            } catch (InterruptedException e) {
                throw new RuntimeException("Failed to download dataset" + datasetId, e);
            }
        }

        String workingCopyId = datasetId + "-" + UUID.randomUUID();
        Path workingCopy = workingDir.resolve(workingCopyId);
        Path workingCopyGraph = workingCopy.resolve(workingCopyId);

        try {
            Files.createDirectories(workingCopyGraph);
            PathUtils.copyDirectory(datasetDir, workingCopyGraph);
        } catch (IOException e) {
            throw new RuntimeException("Could not create working copy", e);
        }

        return openDb(workingCopyGraph);
    }

    public void closeDb(GdsGraphDatabaseAPI db) {
        if (db != null) {
            Path dbDir = db.dbHome(workingDir);
            db.shutdown();
            try {
                PathUtils.deleteDirectory(dbDir);
            } catch (IOException e) {
                throw new RuntimeException("Could not delete working copy", e);
            }
        }
    }

    private GdsGraphDatabaseAPI openDb(Path dbLocation) {
        GdsGraphDatabaseAPI db = dbCreator.createEmbeddedDatabase(dbLocation, neo4jConfig);
        Runtime.getRuntime().addShutdownHook(new Thread(db::shutdown));
        GdsEdition.instance().setToEnterpriseEdition();
        return db;
    }

    public boolean hasDataset(String datasetName) {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(workingDir)) {
            Spliterator<Path> spliterator = directoryStream.spliterator();
            return StreamSupport
                .stream(spliterator, false)
                .anyMatch(folder -> folder.toFile().getName().equals(datasetName));
        } catch (IOException e) {
            throw new RuntimeException("Could not list existing datasets", e);
        }
    }
}
