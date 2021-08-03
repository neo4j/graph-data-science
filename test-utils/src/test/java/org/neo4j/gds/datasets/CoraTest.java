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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.compat.GdsGraphDatabaseAPI;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoraTest {

    @Test
    @GdsEditionTest(Edition.EE)
    void shouldLoadCoraDataset(@TempDir Path tempDir) {
        DatasetManager datasetManager = new DatasetManager(tempDir, CommunityDbCreator.getInstance());
        GdsGraphDatabaseAPI cora = datasetManager.openDb(Cora.ID);
        long nodeCount;
        long propertyKeyCount;
        try (Transaction tx = cora.beginTx()) {
            nodeCount = tx.getAllNodes().stream().count();
            List<String> propertyKeys = Iterables.stream(tx.getAllPropertyKeys()).collect(Collectors.toList());
            propertyKeyCount = propertyKeys.size();
            tx.commit();
        } finally {
            datasetManager.closeDb(cora);
        }
        // The number of features + extId + subject
        assertEquals(1435, propertyKeyCount);
        // 2708 nodes are coming from the cora dataset and 1 additional node is the `magic` node
        // responsible for adding a missing property key
        assertEquals(2709, nodeCount);
    }
}
