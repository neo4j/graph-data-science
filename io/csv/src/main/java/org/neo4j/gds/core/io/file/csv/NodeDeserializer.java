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
package org.neo4j.gds.core.io.file.csv;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.schema.PropertySchema;

import java.io.IOException;
import java.util.Map;

public class NodeDeserializer extends StdDeserializer<NodeDTO> {

    static final String NODE_SCHEMA_INJECTION_NAME = "nodePropertySchemas";

    NodeDeserializer() {
        this(null);
    }

    private NodeDeserializer(@Nullable Class<?> vc) {
        super(vc);
    }

    @Override
    public NodeDTO deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var propertySchemas = (Map<String, PropertySchema>) ctxt.findInjectableValue(NODE_SCHEMA_INJECTION_NAME, null, null);

        var nodeDTO = new NodeDTO();
        var tree = p.readValueAsTree();

        nodeDTO.id = ((JsonNode) tree.get(CsvNodeVisitor.ID_COLUMN_NAME)).asLong();

        var csvSchema = (CsvSchema) p.getSchema();
        csvSchema.forEach(column -> {
            var propertyKey = column.getName();
            if (propertyKey.equals(CsvNodeVisitor.ID_COLUMN_NAME)) {
                return;
            }
            var node = tree.get(propertyKey);
            var propertySchema = propertySchemas.get(propertyKey);
            var parseValue = propertySchema.valueType().fromCsvValue(propertySchema.defaultValue(), (JsonNode) node);
            nodeDTO.setProperty(propertyKey, parseValue);
        });

        return nodeDTO;
    }
}
