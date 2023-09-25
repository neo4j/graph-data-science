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

import de.siegmar.fastcsv.writer.CsvWriter;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.io.schema.SimpleVisitor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;

public class CsvNodeLabelMappingVisitor implements SimpleVisitor<Map.Entry<NodeLabel, String>> {

    private static final String LABEL_MAPPING = "index";
    private static final String LABEL_COLUMN_NAME = "label";
    static final String LABEL_MAPPING_FILE_NAME = "label-mappings.csv";
    private final CsvWriter csvWriter;

    CsvNodeLabelMappingVisitor(Path fileLocation) {
        try {
            this.csvWriter = CsvWriter.builder().build(fileLocation.resolve(LABEL_MAPPING_FILE_NAME), StandardCharsets.UTF_8);
            writeHeader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void export(Map.Entry<NodeLabel, String> nodeLabelMapping) {
        var row = new ArrayList<String>();
        row.add(nodeLabelMapping.getValue());
        row.add(nodeLabelMapping.getKey().name());
        csvWriter.writeRow(row);
    }

    private void writeHeader() {
        csvWriter.writeRow(
            LABEL_MAPPING,
            LABEL_COLUMN_NAME
        );
    }

    @Override
    public void close() throws IOException {
        try {
            csvWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
