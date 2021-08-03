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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphalgo.core.GdsEdition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class DatasetManagerTest {

    @AfterAll
    static void resetToCommunityEdition() {
        GdsEdition.instance().setToCommunityEdition();
    }

    @Test
    void testCloseRemovesDbDirectory(@TempDir Path tempDir) throws IOException {
        var dbCreator = CommunityDbCreator.getInstance();
        DatasetManager datasetManager = new DatasetManager(tempDir, dbCreator);
        EmptyDataset.INSTANCE.generate(tempDir.resolve(EmptyDataset.NAME), dbCreator);
        List<Path> filesWithCachedDataset = Files.walk(tempDir).filter(Files::isRegularFile).collect(toList());

        GdsGraphDatabaseAPI db = datasetManager.openDb(EmptyDataset.NAME);
        datasetManager.closeDb(db);

        List<Path> filesAfterClose = Files.walk(tempDir).filter(Files::isRegularFile).collect(toList());
        assertEquals(filesWithCachedDataset, filesAfterClose, () -> {
            filesAfterClose.removeAll(filesWithCachedDataset);
            return formatWithLocale("found undeleted files: %s", filesAfterClose);
        });
    }

}
