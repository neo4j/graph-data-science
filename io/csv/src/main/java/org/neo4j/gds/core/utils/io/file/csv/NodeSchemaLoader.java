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
package org.neo4j.gds.core.utils.io.file.csv;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.utils.io.file.schema.NodeSchemaBuilderVisitor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class NodeSchemaLoader {

    private final ObjectReader objectReader;
    private final Path nodeSchemaPath;

    NodeSchemaLoader(Path csvDirectory) {
        this.nodeSchemaPath = csvDirectory.resolve(CsvNodeSchemaVisitor.NODE_SCHEMA_FILE_NAME);
        CsvMapper csvMapper = new CsvMapper();
        csvMapper.enable(CsvParser.Feature.TRIM_SPACES);
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        objectReader = csvMapper.readerFor(SchemaLine.class).with(schema);
    }

    NodeSchema load() {
        NodeSchemaBuilderVisitor schemaBuilder = new NodeSchemaBuilderVisitor();

        try(var reader = new BufferedReader(new FileReader(nodeSchemaPath.toFile(), StandardCharsets.UTF_8))) {
            var linesIterator = objectReader.<SchemaLine>readValues(reader);
            while(linesIterator.hasNext()) {
                var schemaLine = linesIterator.next();
                schemaBuilder.nodeLabel(schemaLine.label);
                if (schemaLine.propertyKey != null) {
                    schemaBuilder.key(schemaLine.propertyKey);
                    schemaBuilder.valueType(schemaLine.valueType);
                    schemaBuilder.defaultValue(DefaultValue.of(schemaLine.defaultValue, schemaLine.valueType, true));
                    schemaBuilder.state(schemaLine.state);
                }
                schemaBuilder.endOfEntity();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        schemaBuilder.close();
        return schemaBuilder.schema();
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
        @JsonDeserialize(converter = JacksonConverters.DefaultValueConverter.class)
        String defaultValue;

        @JsonProperty
        PropertyState state;
    }
}
