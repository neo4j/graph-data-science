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
package org.neo4j.gds.api;

import org.neo4j.graphdb.GraphDatabaseService;

import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.neo4j.gds.utils.StringFormatting.toLowerCaseWithLocale;

public final class DatabaseId {

    public static DatabaseId of(GraphDatabaseService databaseService) {
        return new DatabaseId(normalizeDatabseName(databaseService.databaseName()));
    }

    public static DatabaseId from(String databaseName) {
        return new DatabaseId(normalizeDatabseName(databaseName));
    }

    public static DatabaseId random() {
        return from(normalizeDatabseName(UUID.randomUUID().toString()));
    }

    private static String normalizeDatabseName(String databaseName) {
        requireNonNull(databaseName, "Database name should be not null.");
        return toLowerCaseWithLocale(databaseName);
    }

    private final String databaseName;

    public String databaseName() {
        return databaseName;
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) return true;
        return another instanceof DatabaseId && equalTo((DatabaseId) another);
    }

    private boolean equalTo(DatabaseId another) {
        return  databaseName.equals(another.databaseName);
    }

    @Override
    public int hashCode() {
        int h = 5381;
        h += (h << 5) + databaseName.hashCode();
        return h;
    }

    @Override
    public String toString() {
        return "DatabaseId{"
               + "databaseName=" + databaseName
               + "}";
    }

    private DatabaseId(String databaseName) {
        this.databaseName = databaseName;
    }
}
