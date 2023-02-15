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
package org.neo4j.gds.compat;

public final class StorageEngineFactoryIdProvider {

    // Record storage = 0, Freki = 1
    // Let's leave some room for future storage engines
    // This arbitrary seems quite future-proof
    private static final byte ID = 42;

    private StorageEngineFactoryIdProvider() {}

    public static byte id(Neo4jVersion neo4jVersion) {
        // making sure the compatible version comes last when sorting by id
        return Neo4jVersion.findNeo4jVersion() == neo4jVersion ? ID + 1 : ID;
    }
}
