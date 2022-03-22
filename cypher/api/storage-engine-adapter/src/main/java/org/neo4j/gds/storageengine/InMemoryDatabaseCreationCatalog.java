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
package org.neo4j.gds.storageengine;

import org.neo4j.gds.compat.Neo4jProxy;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class InMemoryDatabaseCreationCatalog {
    private static final Map<String, String> CATALOG = new ConcurrentHashMap<>();

    private InMemoryDatabaseCreationCatalog() {}

    public static void registerDbCreation(String databaseName, String graphName) {
        var normalizedDatabaseName = Neo4jProxy.validateExternalDatabaseName(databaseName);
        if (CATALOG.containsKey(normalizedDatabaseName)) {
            throw new IllegalArgumentException(formatWithLocale("An entry with key `%s` already exists", databaseName));
        }
        CATALOG.put(normalizedDatabaseName, graphName);
    }

    public static String getRegisteredDbCreationGraphName(String databaseName) {
        return CATALOG.get(Neo4jProxy.validateExternalDatabaseName(databaseName));
    }

    public static void removeDatabaseEntry(String databaseName) {
        var normalizedDatabaseName = Neo4jProxy.validateExternalDatabaseName(databaseName);
        CATALOG.remove(normalizedDatabaseName);
    }

    public static void removeAllRegisteredDbCreations() {
        CATALOG.clear();
    }

    public static Set<String> databaseNamesRegisteredForCreation() {
        return CATALOG.keySet();
    }
}
