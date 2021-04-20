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
package org.neo4j.gds.ml.splitting;

import org.neo4j.graphalgo.api.Relationships;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EdgeSplitterBaseTest {
    void assertRelExists(Relationships.Topology topology, long source, long... targets) {
        var cursor = topology.list().decompressingCursor(
            topology.offsets().get(source),
            topology.degrees().degree(source)
        );
        for (long target : targets) {
            assertEquals(target, cursor.nextVLong());
        }
    }

    void assertRelProperties(Relationships.Properties properties, long source, double... values) {
        var cursor = properties.list().cursor(
            properties.offsets().get(source),
            properties.degrees().degree(source)
        );
        for (double property : values) {
            assertEquals(property, Double.longBitsToDouble(cursor.nextLong()));
        }
    }
}
