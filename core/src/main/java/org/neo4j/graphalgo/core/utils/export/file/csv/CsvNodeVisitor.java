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

import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.core.utils.export.file.NodeVisitor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class CsvNodeVisitor extends NodeVisitor {

    public static final String ID_COLUMN_NAME = ":ID";
    public static final String REVERSE_ID_COLUMN_NAME = ":NEO_ID";

    private final Path fileLocation;
    private final int visitorId;
    private final Map<String, CsvAppender> csvAppenders;
    private final CsvWriter csvWriter;
    private final Set<String> headerFiles;
    private final NodeIdFieldAppender nodeIdFieldAppender;

    public CsvNodeVisitor(
        Path fileLocation,
        NodeSchema nodeSchema,
        Set<String> headerFiles,
        int visitorId,
        boolean reverseIdMapping
    ) {
        super(nodeSchema);
        this.fileLocation = fileLocation;
        this.headerFiles = headerFiles;
        this.visitorId = visitorId;
        this.csvAppenders = new HashMap<>();
        this.csvWriter = new CsvWriter();
        NodeIdFieldAppender internalIdAppender = this::appendInternalId;
        this.nodeIdFieldAppender = reverseIdMapping
            ? internalIdAppender.andThen(this::appendNeo4jId)
            : internalIdAppender;
    }

    @TestOnly
    public CsvNodeVisitor(Path fileLocation, NodeSchema nodeSchema, boolean reverseIdMapping) {
        this(fileLocation, nodeSchema, new HashSet<>(), 0, reverseIdMapping);
    }

    @Override
    protected void exportElement() {
        // do the export
        var csvAppender = getAppender();
        try {
            nodeIdFieldAppender.appendIdField(csvAppender);

            // write properties
            forEachProperty(((key, value, type) -> {
                var propertyString = type.csvValue(value);
                try {
                    csvAppender.appendField(propertyString);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));

            csvAppender.endLine();
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

    private void appendInternalId(CsvAppender csvAppender) throws IOException {
        csvAppender.appendField(Long.toString(id()));
    }

    private void appendNeo4jId(CsvAppender csvAppender) throws IOException {
        csvAppender.appendField(Long.toString(originalId()));
    }

    private CsvAppender getAppender() {
        var labelsString = String.join("_", labels());

        return csvAppenders.computeIfAbsent(labelsString, (ignore) -> {
            var fileName = labelsString.isBlank() ? "nodes" : formatWithLocale("nodes_%s", labelsString);
            var headerFileName = formatWithLocale("%s_header.csv", fileName);
            var dataFileName = formatWithLocale("%s_%d.csv", fileName, visitorId);

            if (headerFiles.add(headerFileName)) {
                writeHeaderFile(headerFileName);
            }

            try {
                return csvWriter.append(fileLocation.resolve(dataFileName), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void writeHeaderFile(String headerFileName) {
        try (var headerAppender = csvWriter.append(fileLocation.resolve(headerFileName), StandardCharsets.UTF_8)) {
            headerAppender.appendField(ID_COLUMN_NAME);
            headerAppender.appendField(REVERSE_ID_COLUMN_NAME);

            forEachProperty(((key, value, type) -> {
                var propertyHeader = formatWithLocale(
                    "%s:%s",
                    key,
                    type.csvName()
                );
                try {
                    headerAppender.appendField(propertyHeader);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));

            headerAppender.endLine();
        } catch (IOException e) {
            throw new RuntimeException("Could not write header file", e);
        }
    }

    @FunctionalInterface
    interface NodeIdFieldAppender {
        void appendIdField(CsvAppender csvAppender) throws IOException;

        default NodeIdFieldAppender andThen(NodeIdFieldAppender nextNodeIdFieldAppender) {
            return (csvAppender -> {
                appendIdField(csvAppender);
                nextNodeIdFieldAppender.appendIdField(csvAppender);
            });
        }
    }
}
