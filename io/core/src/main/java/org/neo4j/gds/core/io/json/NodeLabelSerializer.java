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

import org.neo4j.gds.NodeLabel;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;

public class NodeLabelSerializer extends ValueSerializer<NodeLabel> {

    @Override
    public void serialize(NodeLabel value, JsonGenerator gen, SerializationContext serializers) {
        gen.writeString(value.name());
    }

    public static class KeySerializer extends ValueSerializer<NodeLabel> {
        @Override
        public void serialize(NodeLabel value, JsonGenerator gen, SerializationContext serializers) {
            gen.writeName(value.name());
        }
    }
}
