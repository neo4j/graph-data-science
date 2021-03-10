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
import org.neo4j.graphalgo.core.utils.export.file.schema.NodeSchemaVisitor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class CsvNodeSchemaVisitor extends NodeSchemaVisitor {

    static final String LABEL_COLUMN_NAME = "label";
    static final String PROPERTY_KEY_COLUMN_NAME = "propertyKey";
    static final String VALUE_TYPE_COLUMN_NAME = "valueType";
    static final String DEFAULT_VALUE_COLUMN_NAME = "defaultValue";
    static final String STATE_COLUMN_NAME = "state";
    static final String NODE_SCHEMA_FILE_NAME = "node-schema.csv";

    private final CsvAppender csvAppender;

    public CsvNodeSchemaVisitor(Path fileLocation) {
        try {
            this.csvAppender = new CsvWriter().append(fileLocation.resolve(NODE_SCHEMA_FILE_NAME), StandardCharsets.UTF_8);
            writeHeader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void export() {
        try {
            csvAppender.appendField(nodeLabel().name());
            csvAppender.appendField(key());
            csvAppender.appendField(valueType().csvName());
            csvAppender.appendField(defaultValue().toString());
            csvAppender.appendField(state().name());
            csvAppender.endLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        csvAppender.flush();
        csvAppender.close();
    }

    private void writeHeader() throws IOException {
        csvAppender.appendField(LABEL_COLUMN_NAME);
        csvAppender.appendField(PROPERTY_KEY_COLUMN_NAME);
        csvAppender.appendField(VALUE_TYPE_COLUMN_NAME);
        csvAppender.appendField(DEFAULT_VALUE_COLUMN_NAME);
        csvAppender.appendField(STATE_COLUMN_NAME);
        csvAppender.endLine();
    }
}
