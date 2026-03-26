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

import com.fasterxml.jackson.databind.JsonSerializer;
import org.neo4j.gds.api.DatabaseInfo;

public class DatabaseInfoSerializer extends JsonSerializer<DatabaseInfo> {

    @Override
    public void serialize(DatabaseInfo value, com.fasterxml.jackson.core.JsonGenerator gen, com.fasterxml.jackson.databind.SerializerProvider serializers) throws java.io.IOException {
        gen.writeStartObject();
        gen.writeFieldName("databaseId");
        serializers.defaultSerializeValue(value.databaseId(), gen);
        gen.writeFieldName("databaseLocation");
        serializers.defaultSerializeValue(value.databaseLocation(), gen);
        if (value.remoteDatabaseId().isPresent()) {
            gen.writeFieldName("remoteDatabaseId");
            serializers.defaultSerializeValue(value.remoteDatabaseId().get(), gen);
        }
        gen.writeEndObject();
    }
}
