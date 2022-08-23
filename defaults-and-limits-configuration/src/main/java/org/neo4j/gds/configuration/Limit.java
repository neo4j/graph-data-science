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
package org.neo4j.gds.configuration;

/**
 * For now we support int, this will need to change.
 *
 * And we are prepared, the {@link org.neo4j.gds.configuration.Limit#isViolated(Object)} method ought to be generic
 * enough that we can turn this into an interface and back it with supported types, or something
 */
public class Limit {
    private final long value;

    public Limit(long value) {
        this.value = value;
    }

    boolean isViolated(Object inputValue) {
        long i = (Long) inputValue;

        return i > value;
    }

    String getValueAsString() {
        return String.valueOf(value);
    }

    Object getValue() {
        return value;
    }
}
