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
package org.neo4j.gds.core.io.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;

import java.io.IOException;
import java.util.Optional;

public class DatabaseInfoDeserializer extends JsonDeserializer<DatabaseInfo> {
    @Override
    public DatabaseInfo deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        DatabaseId databaseId = null;
        DatabaseInfo.DatabaseLocation databaseLocation = null;
        Optional<DatabaseId> remoteDatabaseId = Optional.empty();

        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected START_OBJECT, found " + p.currentToken());
        }

        while (p.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = p.currentName();
            p.nextToken();

            switch (fieldName) {
                case "databaseId":
                    databaseId = ctxt.readValue(p, DatabaseId.class);
                    break;
                case "databaseLocation":
                    databaseLocation = ctxt.readValue(p, DatabaseInfo.DatabaseLocation.class);
                    break;
                case "remoteDatabaseId":
                    remoteDatabaseId = Optional.ofNullable(ctxt.readValue(p, DatabaseId.class));
                    break;
                default:
                    p.skipChildren(); // Ignore unknown fields
                    break;
            }
        }

        if (databaseId == null) {
            throw new IOException("Missing required field 'databaseId'");
        }
        if (databaseLocation == null) {
            throw new IOException("Missing required field 'databaseLocation'");
        }

        return remoteDatabaseId.isPresent()
            ? DatabaseInfo.of(databaseId, databaseLocation, remoteDatabaseId.get())
            : DatabaseInfo.of(databaseId, databaseLocation);
    }
}
