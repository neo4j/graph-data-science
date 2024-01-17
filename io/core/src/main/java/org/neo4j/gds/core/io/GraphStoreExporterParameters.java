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
package org.neo4j.gds.core.io;

public final class GraphStoreExporterParameters {
    public static GraphStoreExporterParameters create(String defaultRelationshipType, int batchSize, int concurrency) {
        return new GraphStoreExporterParameters(defaultRelationshipType, batchSize, concurrency);
    }

    private final String defaultRelationshipType;
    private final int batchSize;
    private final int concurrency;

    private GraphStoreExporterParameters(String defaultRelationshipType, int batchSize, int concurrency) {
        this.defaultRelationshipType = defaultRelationshipType;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
    }

    public String defaultRelationshipType() {
        return defaultRelationshipType;
    }

    int batchSize() {
        return batchSize;
    }

    public int concurrency() {
        return concurrency;
    }
}
