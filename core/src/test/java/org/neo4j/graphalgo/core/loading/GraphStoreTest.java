/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphStoreTest extends AlgoTestBase {

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(
            "CREATE (a)-[:REL]->(b)"
        );
    }

    @Test
    void testModificationDate() {
        GraphStore graphStore = new StoreLoaderBuilder().api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .build()
            .graphStore(NativeFactory.class);

        // add node properties
        LocalDateTime initial = graphStore.modificationTime();
        graphStore.addNodeProperty("foo", new NullPropertyMap(42.0));
        LocalDateTime afterNodePropertyUpdate = graphStore.modificationTime();

        // add relationships
        HugeGraph.Relationships relationships = HugeGraph.Relationships.of(
            0L,
            Orientation.NATURAL,
            new AdjacencyList(new byte[0][0]),
            AdjacencyOffsets.of(new long[0]),
            null,
            null,
            42.0
        );
        graphStore.addRelationshipType("BAR", Optional.empty(), relationships);
        LocalDateTime afterRelationshipsUpdate = graphStore.modificationTime();

        assertTrue(initial.isBefore(afterNodePropertyUpdate));
        assertTrue(afterNodePropertyUpdate.isBefore(afterRelationshipsUpdate));
    }

}