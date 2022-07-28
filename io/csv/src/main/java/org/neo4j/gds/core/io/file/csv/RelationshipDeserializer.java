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

public class RelationshipDeserializer extends StdDeserializer<RelationshipDTO> {

    static final String RELATIONSHIP_SCHEMA_INJECTION_NAME = "relationshipPropertySchemas";

    RelationshipDeserializer() {
        this(null);
    }

    private RelationshipDeserializer(@Nullable Class<?> vc) {
        super(vc);
    }

    @Override
    public RelationshipDTO deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var propertySchemas = (Map<String, PropertySchema>) ctxt.findInjectableValue(
            RELATIONSHIP_SCHEMA_INJECTION_NAME,
            null,
            null
        );

        var relationshipDTO = new RelationshipDTO();
        var tree = p.readValueAsTree();

        relationshipDTO.sourceId = ((JsonNode) tree.get(CsvRelationshipVisitor.START_ID_COLUMN_NAME)).asLong();
        relationshipDTO.targetId = ((JsonNode) tree.get(CsvRelationshipVisitor.END_ID_COLUMN_NAME)).asLong();

        var csvSchema = (CsvSchema) p.getSchema();
        csvSchema.forEach(column -> {
            var propertyKey = column.getName();
            if (propertyKey.equals(CsvRelationshipVisitor.START_ID_COLUMN_NAME) || propertyKey.equals(CsvRelationshipVisitor.END_ID_COLUMN_NAME)) {
                return;
            }
            var node = tree.get(propertyKey);
            var propertySchema = propertySchemas.get(propertyKey);
            var parseValue = CsvImportParsingUtil
                .getParsingFunction(propertySchema.valueType())
                .fromCsvValue(propertySchema.defaultValue(), (JsonNode) node);
            relationshipDTO.setProperty(propertyKey, parseValue);
        });

        return relationshipDTO;
    }
}
