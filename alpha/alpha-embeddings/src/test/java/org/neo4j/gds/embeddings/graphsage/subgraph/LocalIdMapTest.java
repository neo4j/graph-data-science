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
package org.neo4j.gds.embeddings.graphsage.subgraph;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMapImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LocalIdMapTest {
    @Test
    void shouldMapCorrectly() {
        LocalIdMap localIdMap = new LocalIdMapImpl();

        localIdMap.toMapped(42L);

        assertEquals(0, localIdMap.toMapped(42L));
        assertEquals(42L, localIdMap.toOriginal(0));

        localIdMap.toMapped(24L);

        assertEquals(0, localIdMap.toMapped(42L));
        assertEquals(42L, localIdMap.toOriginal(0));


        assertEquals(1, localIdMap.toMapped(24L));
        assertEquals(24L, localIdMap.toOriginal(1));
    }

    @Test
    void testAddingManyNumbers() {
        LocalIdMap localIdMap = new LocalIdMapImpl();

        for (int i = 100; i < 200; i++) {
            int id = localIdMap.toMapped(i);
            assertEquals(i - 100, id);
            assertEquals(i, localIdMap.toOriginal(id));
            if (i % 3 == 1) {
                assertEquals(id,localIdMap.toMapped(i));
            }
        }
    }

}