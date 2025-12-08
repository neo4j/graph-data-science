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
package org.neo4j.gds.core;

/**
 * This username microtype is very much tied to Neo4j.
 * It is the type we inject in Pregel and places.
 */
public final class Username {
    /**
     * In Neo4j, the anonymous user has <i>blank</i> username.
     */
    public static final Username EMPTY_USERNAME = new Username("");

    private final String value;

    public static Username of(String username) {
        return new Username(username);
    }

    private Username(String value) {
        this.value = value;
    }

    public String username() {
        return value;
    }
}
