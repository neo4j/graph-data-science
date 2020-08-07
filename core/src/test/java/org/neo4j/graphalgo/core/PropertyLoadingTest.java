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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.Graph;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PropertyLoadingTest extends BaseTest {

    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node { longProp: 42, doubleProp: 13.37, longListProp: [0, 1], doubleListProp: [0.1, 1.2] })" +
        ", (b:Node)";

    Graph graph;

    @BeforeEach
    void setup() {
        db.executeTransactionally(DB_CYPHER);
        graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeProperty(PropertyMapping.of("longProp", 24L))
            .addNodeProperty(PropertyMapping.of("doubleProp", 73.31D))
            .addNodeProperty(PropertyMapping.of("longListProp", new long[]{ 0L }))
            .addNodeProperty(PropertyMapping.of("doubleListProp", new double[]{ 0.0D }))
            .build()
            .graph();
    }

    @Test
    void loadDifferentPropertyTypesWithDefaults() {
        assertEquals(42L, graph.nodeProperties("longProp").getLong(0));
        assertEquals(24L, graph.nodeProperties("longProp").getLong(1));

        assertEquals(13.37D, graph.nodeProperties("doubleProp").getDouble(0));
        assertEquals(73.31D, graph.nodeProperties("doubleProp").getDouble(1));

        assertArrayEquals(new long[]{ 0, 1 }, graph.nodeProperties("longListProp").getLongArray(0));
        assertArrayEquals(new long[]{ 0 }, graph.nodeProperties("longListProp").getLongArray(1));

        assertArrayEquals(new double[]{ 0.1, 1.2 }, graph.nodeProperties("doubleListProp").getDoubleArray(0));
        assertArrayEquals(new double[]{ 0.0 }, graph.nodeProperties("doubleListProp").getDoubleArray(1));
    }
}
