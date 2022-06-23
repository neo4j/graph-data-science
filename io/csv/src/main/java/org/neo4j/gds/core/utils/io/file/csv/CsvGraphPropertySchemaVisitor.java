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

import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.neo4j.gds.core.io.schema.ElementSchemaVisitor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.neo4j.gds.core.utils.io.file.csv.CsvNodeSchemaVisitor.DEFAULT_VALUE_COLUMN_NAME;
import static org.neo4j.gds.core.utils.io.file.csv.CsvNodeSchemaVisitor.PROPERTY_KEY_COLUMN_NAME;
import static org.neo4j.gds.core.utils.io.file.csv.CsvNodeSchemaVisitor.STATE_COLUMN_NAME;
import static org.neo4j.gds.core.utils.io.file.csv.CsvNodeSchemaVisitor.VALUE_TYPE_COLUMN_NAME;

public class CsvGraphPropertySchemaVisitor extends ElementSchemaVisitor  {

    public static final String GRAPH_PROPERTY_SCHEMA_FILE_NAME = "graph-property-schema.csv";

    private final CsvAppender csvAppender;

    public CsvGraphPropertySchemaVisitor(Path fileLocation) {
        try {
            this.csvAppender = new CsvWriter().append(fileLocation.resolve(GRAPH_PROPERTY_SCHEMA_FILE_NAME), StandardCharsets.UTF_8);
            writeHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void export() {
        try {
            if (key() != null) {
                csvAppender.appendField(key());
                csvAppender.appendField(valueType().csvName());
                csvAppender.appendField(defaultValue().toString());
                csvAppender.appendField(state().name());
            }
            csvAppender.endLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            csvAppender.flush();
            csvAppender.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeHeader() throws IOException {
        csvAppender.appendField(PROPERTY_KEY_COLUMN_NAME);
        csvAppender.appendField(VALUE_TYPE_COLUMN_NAME);
        csvAppender.appendField(DEFAULT_VALUE_COLUMN_NAME);
        csvAppender.appendField(STATE_COLUMN_NAME);
        csvAppender.endLine();
    }
}
