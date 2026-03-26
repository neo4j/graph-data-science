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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;

import java.io.IOException;

public class CapabilitiesDeserializer extends JsonDeserializer<Capabilities> {
    @Override
    public Capabilities deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected START_OBJECT, found " + p.currentToken());
        }

        Capabilities.WriteMode writeMode = null;

        while (p.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = p.currentName();
            p.nextToken();

            if (fieldName.equals("writeMode")) {
                writeMode = ctxt.readValue(p, Capabilities.WriteMode.class);
            } else {
                p.skipChildren(); // Ignore unknown fields
            }
        }

        if (writeMode == null) {
            throw new IOException("Missing required field 'writeMode'");
        }

        return ImmutableStaticCapabilities.of(writeMode);

    }
}
