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
package org.neo4j.gds.applications.graphstorecatalog;

import java.util.Map;
import java.util.Optional;

public final class WriteRelationshipResult {
    public final long writeMillis;
    public final String graphName;
    public final String relationshipType;
    public final String relationshipProperty;
    public final long relationshipsWritten;
    public final long propertiesWritten;
    public final Map<String, Object> configuration;

    private WriteRelationshipResult(
        long writeMillis,
        String graphName,
        String relationshipType,
        Optional<String> relationshipProperty,
        long relationshipsWritten,
        Map<String, Object> configuration
    ) {
        this.writeMillis = writeMillis;
        this.graphName = graphName;
        this.relationshipType = relationshipType;
        this.relationshipProperty = relationshipProperty.orElse(null);
        this.relationshipsWritten = relationshipsWritten;
        this.configuration = configuration;
        this.propertiesWritten = relationshipProperty.isPresent() ? relationshipsWritten : 0L;
    }

    static class Builder {
        private final String graphName;
        private final String relationshipType;
        private final Optional<String> maybeRelationshipProperty;

        private long writeMillis;
        private long relationshipsWritten;
        private Map<String, Object> configuration;

        Builder(String graphName, String relationshipType, Optional<String> maybeRelationshipProperty) {
            this.graphName = graphName;
            this.relationshipType = relationshipType;
            this.maybeRelationshipProperty = maybeRelationshipProperty;
        }

        Builder withWriteMillis(long writeMillis) {
            this.writeMillis = writeMillis;
            return this;
        }

        Builder withRelationshipsWritten(long relationshipsWritten) {
            this.relationshipsWritten = relationshipsWritten;
            return this;
        }

        Builder withConfiguration(Map<String, Object> configuration) {
            this.configuration = configuration;
            return this;
        }

        WriteRelationshipResult build() {
            return new WriteRelationshipResult(
                writeMillis,
                graphName,
                relationshipType,
                maybeRelationshipProperty,
                relationshipsWritten,
                configuration
            );
        }
    }
}
