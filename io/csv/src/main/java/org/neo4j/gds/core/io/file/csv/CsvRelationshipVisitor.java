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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.core.io.file.RelationshipVisitor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CsvRelationshipVisitor extends RelationshipVisitor {

    public static final String START_ID_COLUMN_NAME = ":START_ID";
    public static final String END_ID_COLUMN_NAME = ":END_ID";

    private final Path fileLocation;
    private final Set<String> headerFiles;
    private final int visitorId;
    private final Map<String, JacksonFileAppender> csvAppenders;

    CsvRelationshipVisitor(
        Path fileLocation,
        RelationshipSchema relationshipSchema,
        Set<String> headerFiles,
        int visitorId
    ) {
        super(relationshipSchema);
        this.fileLocation = fileLocation;
        this.headerFiles = headerFiles;
        this.visitorId = visitorId;
        this.csvAppenders = new HashMap<>();
    }

    @TestOnly
    CsvRelationshipVisitor(Path fileLocation, RelationshipSchema relationshipSchema) {
        this(fileLocation, relationshipSchema, new HashSet<>(), 0);
    }

    @Override
    protected void exportElement() {
        // do the import
        var fileAppender = getAppender();
        try {
            fileAppender.startLine();
            // write start and end nodes
            fileAppender.append(startNode());
            fileAppender.append(endNode());

            // write properties
            forEachProperty(((key, value) -> {
                try {
                    fileAppender.appendAny(value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }));

            fileAppender.endLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
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

    private JacksonFileAppender getAppender() {
        return csvAppenders.computeIfAbsent(relationshipType(), (ignore) -> {
            var fileName = formatWithLocale("relationships_%s", relationshipType());
            var headerFileName = formatWithLocale("%s_header.csv", fileName);
            var dataFileName = formatWithLocale("%s_%d.csv", fileName, visitorId);

            if (headerFiles.add(headerFileName)) {
                writeHeaderFile(headerFileName);
            }

            return fileAppender(fileLocation.resolve(dataFileName));
        });
    }

    private void writeHeaderFile(String headerFileName) {
        try (var headerAppender = fileAppender(fileLocation.resolve(headerFileName))) {
            headerAppender.startLine();
            headerAppender.append(START_ID_COLUMN_NAME);
            headerAppender.append(END_ID_COLUMN_NAME);

            forEachPropertyWithType(((key, value, type) -> {
                var propertyHeader = formatWithLocale(
                    "%s:%s",
                    key,
                    type.csvName()
                );
                try {
                    headerAppender.append(propertyHeader);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));

            headerAppender.endLine();
        } catch (IOException e) {
            throw new RuntimeException("Could not write header file", e);
        }
    }

    @Override
    public void flush() throws IOException {
        for (var csvAppender : csvAppenders.values()) {
            csvAppender.flush();
        }
    }

    private JacksonFileAppender fileAppender(Path filePath) {
        var propertySchema = getPropertySchema();
        propertySchema.sort(Comparator.comparing(PropertySchema::key));
        return JacksonFileAppender.of(
            filePath,
            propertySchema,
            csvSchemaBuilder -> csvSchemaBuilder
                .addNumberColumn(START_ID_COLUMN_NAME)
                .addNumberColumn(END_ID_COLUMN_NAME)
        );
    }
}
