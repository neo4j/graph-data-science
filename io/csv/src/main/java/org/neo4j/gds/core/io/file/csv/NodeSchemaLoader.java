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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.io.schema.NodeSchemaBuilderVisitor;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.dataformat.csv.CsvMapper;
import tools.jackson.dataformat.csv.CsvReadFeature;
import tools.jackson.dataformat.csv.CsvSchema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.OptionalInt;

public class NodeSchemaLoader {

    private final ObjectReader objectReader;
    private final Path nodeSchemaPath;

    public NodeSchemaLoader(Path csvDirectory) {
        this.nodeSchemaPath = csvDirectory.resolve(CsvNodeSchemaVisitor.NODE_SCHEMA_FILE_NAME);
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        this.objectReader = CsvMapper.builder()
            .enable(CsvReadFeature.TRIM_SPACES)
            .build()
            .readerFor(SchemaLine.class)
            .with(schema);
    }

    public NodeSchema load() {
        try (var schemaBuilder = new NodeSchemaBuilderVisitor();
             var reader = new BufferedReader(new FileReader(nodeSchemaPath.toFile(), StandardCharsets.UTF_8))
        ) {
            var linesIterator = objectReader.<SchemaLine>readValues(reader);
            while(linesIterator.hasNext()) {
                var schemaLine = linesIterator.next();
                schemaBuilder.nodeLabel(schemaLine.label);
                if (schemaLine.propertyKey != null) {
                    schemaBuilder.key(schemaLine.propertyKey);
                    schemaBuilder.valueType(schemaLine.valueType);
                    schemaBuilder.defaultValue(DefaultValueIOHelper.deserialize(schemaLine.defaultValue, schemaLine.valueType, true));
                    schemaBuilder.state(schemaLine.state);
                    schemaBuilder.dimension(parseDimension(schemaLine.dimension));
                }
                schemaBuilder.endOfEntity();
            }

            return schemaBuilder.schema();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static OptionalInt parseDimension(String dimension) {
        return dimension == null || dimension.isBlank()
            ? OptionalInt.empty()
            : OptionalInt.of(Integer.parseInt(dimension.trim()));
    }

    public static class SchemaLine {

        @JsonProperty
        @JsonDeserialize(converter = JacksonConverters.NodeLabelConverter.class)
        NodeLabel label;

        @JsonProperty
        String propertyKey;

        @JsonProperty
        @JsonDeserialize(converter = JacksonConverters.ValueTypeConverter.class)
        ValueType valueType;

        @JsonProperty
        String defaultValue;

        @JsonProperty
        PropertyState state;

        @JsonProperty
        String dimension;
    }
}
