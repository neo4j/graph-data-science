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

import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.io.file.GraphPropertyVisitor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CsvGraphPropertyVisitor extends GraphPropertyVisitor {

    static final String GRAPH_PROPERTY_DATA_FILE_NAME_TEMPLATE = "graph_property_%s_%d.csv";
    private static final String GRAPH_PROPERTY_HEADER_FILE_NAME_TEMPLATE = "graph_property_%s_header.csv";

    private final Path fileLocation;
    private final Map<String, PropertySchema> graphPropertySchemas;
    private final int visitorId;
    private final Map<String, JacksonFileAppender> csvAppenders;
    private final Set<String> headerFiles;

    public CsvGraphPropertyVisitor(
        Path fileLocation,
        Map<String, PropertySchema> graphPropertySchemas,
        Set<String> headerFiles,
        int visitorId
    ) {
        this.fileLocation = fileLocation;
        this.graphPropertySchemas = graphPropertySchemas;
        this.headerFiles = headerFiles;
        this.visitorId = visitorId;
        this.csvAppenders = new HashMap<>();
    }

    @Override
    public boolean property(String key, Object value) {
        var appender = getAppender(key);
        try {
            appender.startLine();
            appender.appendAny(value);
            appender.endLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public void flush() throws IOException {
        for (var csvAppender : csvAppenders.values()) {
            csvAppender.flush();
        }
    }

    @Override
    public void close() {
        csvAppenders.values().forEach(csvAppender -> {
            try {
                csvAppender.flush();
                csvAppender.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private JacksonFileAppender getAppender(String propertyKey) {
        return csvAppenders.computeIfAbsent(propertyKey, __ -> {
            var headerFileName = formatWithLocale(GRAPH_PROPERTY_HEADER_FILE_NAME_TEMPLATE, propertyKey);
            var dataFileName = formatWithLocale(GRAPH_PROPERTY_DATA_FILE_NAME_TEMPLATE, propertyKey, visitorId);
            var propertySchema = graphPropertySchemas.get(propertyKey);

            if (headerFiles.add(headerFileName)) {
                writeHeaderFile(propertySchema, headerFileName);
            }

            return fileAppender(fileLocation.resolve(dataFileName), propertySchema);
        });
    }

    private void writeHeaderFile(PropertySchema propertySchema, String headerFileName) {
        try (var headerAppender = fileAppender(fileLocation.resolve(headerFileName), propertySchema)) {
            var propertyHeader = formatWithLocale(
                "%s:%s",
                propertySchema.key(),
                propertySchema.valueType().csvName()
            );

            headerAppender.startLine();
            headerAppender.append(propertyHeader);
            headerAppender.endLine();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write header file", e);
        }
    }

    private JacksonFileAppender fileAppender(
        Path filePath,
        PropertySchema propertySchema
    ) {
        return JacksonFileAppender.of(
            filePath,
            List.of(propertySchema),
            UnaryOperator.identity()
        );
    }
}
