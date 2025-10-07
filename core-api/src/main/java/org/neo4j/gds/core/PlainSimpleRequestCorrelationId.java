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

import java.util.UUID;

/**
 * If you are not, or do not want to, integrate with external systems,
 * then this is a perfectly good correlation id to use.
 * It will look something like "gid-74b4d", which is descriptive, and unique enough for the lifetime of a GDS instance.
 */
public class PlainSimpleRequestCorrelationId implements RequestCorrelationId {
    private final String id = "gid-" + UUID.randomUUID().toString().substring(0, 5);

    @Override
    public String toString() {
        return id;
    }
}
