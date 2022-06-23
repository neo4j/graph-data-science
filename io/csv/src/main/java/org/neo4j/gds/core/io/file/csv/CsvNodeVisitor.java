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

import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.io.file.NodeVisitor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CsvNodeVisitor extends NodeVisitor {

    public static final String ID_COLUMN_NAME = ":ID";

    private final Path fileLocation;
    private final int visitorId;
    private final Map<String, JacksonFileAppender> csvAppenders;
    private final Set<String> headerFiles;

    CsvNodeVisitor(
        Path fileLocation,
        NodeSchema nodeSchema,
        Set<String> headerFiles,
        int visitorId
    ) {
        super(nodeSchema);
        this.fileLocation = fileLocation;
        this.headerFiles = headerFiles;
        this.visitorId = visitorId;
        this.csvAppenders = new HashMap<>();
    }

    @TestOnly
    public CsvNodeVisitor(Path fileLocation, NodeSchema nodeSchema) {
        this(fileLocation, nodeSchema, new HashSet<>(), 0);
    }

    @Override
    protected void exportElement() {
        // do the export
        var fileAppender = getAppender();

        try {
            fileAppender.startLine();
            fileAppender.append(id());
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
        var labelsString = elementIdentifier();

        return csvAppenders.computeIfAbsent(labelsString, (ignore) -> {
            var fileName = labelsString.isBlank() ? "nodes" : formatWithLocale("nodes_%s", labelsString);
            var headerFileName = formatWithLocale("%s_header.csv", fileName);
            var dataFileName = formatWithLocale("%s_%d.csv", fileName, visitorId);

            if (headerFiles.add(headerFileName)) {
                writeHeaderFile(headerFileName);
            }

            return fileAppender(
                fileLocation.resolve(dataFileName),
                csvSchemaBuilder -> csvSchemaBuilder.addNumberColumn(ID_COLUMN_NAME)
            );
        });
    }

    private void writeHeaderFile(String headerFileName) {
        try (var headerAppender = fileAppender(
            fileLocation.resolve(headerFileName),
            csvSchemaBuilder -> csvSchemaBuilder.addColumn(ID_COLUMN_NAME, CsvSchema.ColumnType.STRING)
        )) {
            headerAppender.startLine();
            headerAppender.append(ID_COLUMN_NAME);

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

    private JacksonFileAppender fileAppender(Path filePath, UnaryOperator<CsvSchema.Builder> builderUnaryOperator) {
        var propertySchema = getPropertySchema();
        propertySchema.sort(Comparator.comparing(PropertySchema::key));
        return JacksonFileAppender.of(
            filePath,
            propertySchema,
            builderUnaryOperator
        );
    }
}
