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
package org.neo4j.graphalgo.core.utils.export.file.csv;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.api.schema.ElementSchema;
import org.neo4j.graphalgo.api.schema.PropertySchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class JacksonGeneratorFileAppender<ELEMENT_IDENTIFIER extends ElementIdentifier, PROPERTY_SCHEMA extends PropertySchema> implements FileAppender {

    private final CsvGenerator csvEncoder;
    private final CsvSchema csvSchema;

    private int currentColumnIndex = 0;

    public JacksonGeneratorFileAppender(Path filePath, ElementSchema<?, ELEMENT_IDENTIFIER, PROPERTY_SCHEMA> elementSchema, Stream<? extends ELEMENT_IDENTIFIER> elements, UnaryOperator<CsvSchema.Builder> schemaEnricher) {
        List<PropertySchema> propertySchemaForLabels;

            var schemaProperties = elementSchema.properties();
            var propertySchemaForElementsMap = elements.flatMap(element -> {
                var properties = schemaProperties.getOrDefault(element, Map.of());
                return properties.entrySet().stream();
            }).collect(Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.collectingAndThen(
                    Collectors.reducing(
                        (leftSchema, rightSchema) -> {
                            if (leftSchema.getValue().valueType() != rightSchema.getValue().valueType()) {
                                throw new IllegalArgumentException(formatWithLocale(
                                    "Combining schema entries with value type %s and %s is not supported.",
                                    leftSchema.getValue().valueType(),
                                    rightSchema.getValue().valueType()
                                ));
                            } else {
                                return leftSchema;
                            }
                        }
                    ),
                    schemas -> schemas.get().getValue()
                )
            ));

            propertySchemaForLabels = new ArrayList<>(propertySchemaForElementsMap.values());
            propertySchemaForLabels.sort(Comparator.comparing(PropertySchema::key));


        var csvSchemaBuilder = schemaEnricher.apply(CsvSchema.builder());
        for (PropertySchema propertySchemaForLabel : propertySchemaForLabels) {
            switch (propertySchemaForLabel.valueType()) {
                case LONG:
                case DOUBLE:
                    csvSchemaBuilder.addNumberColumn(propertySchemaForLabel.key());
                    break;
                case DOUBLE_ARRAY:
                case FLOAT_ARRAY:
                case LONG_ARRAY:
                    csvSchemaBuilder.addArrayColumn(propertySchemaForLabel.key(), ";");
                    break;
                case UNKNOWN:
                    break;
            }
        }
        csvSchema = csvSchemaBuilder.build();

        try {
            var mapper = CsvMapper.csvBuilder().build();
            csvEncoder = mapper.getFactory().createGenerator(filePath.toFile(), JsonEncoding.UTF8);
            csvEncoder.setSchema(csvSchema);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void append(long value) throws IOException {
        setFieldName();
        csvEncoder.writeNumber(value);
    }

    @Override
    public void append(double value) throws IOException {
        setFieldName();
        csvEncoder.writeNumber(value);
    }

    @Override
    public void append(String value) throws IOException {
        setFieldName();
        csvEncoder.writeString(value);
    }

    @Override
    public void append(double[] value) throws IOException {
        setFieldName();
        csvEncoder.writeArray(value, 0, value.length);
    }

    @Override
    public void append(long[] value) throws IOException {
        setFieldName();
        csvEncoder.writeArray(value, 0, value.length);
    }

    @Override
    public void append(float[] value) throws IOException {
        setFieldName();
        csvEncoder.writeStartArray(value, value.length);
        for (float v : value) {
            csvEncoder.writeNumber(v);
        }
        csvEncoder.writeEndArray();
    }

    @Override
    public void startLine() throws IOException {
        currentColumnIndex = 0;
        csvEncoder.writeStartObject();
    }

    @Override
    public void endLine() throws IOException {
        csvEncoder.writeEndObject();
    }

    @Override
    public void flush() throws IOException {
        csvEncoder.flush();
    }

    @Override
    public void close() throws IOException {
        csvEncoder.close();
    }

    private void setFieldName() throws IOException {
        var column = csvSchema.column(currentColumnIndex++);
        csvEncoder.writeFieldName(column.getName());
    }
}
