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
import org.neo4j.graphalgo.core.utils.export.file.schema.RelationshipSchemaVisitor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class CsvRelationshipSchemaVisitor extends RelationshipSchemaVisitor {

    private static final String RELATIONSHIP_SCHEMA_FILE_NAME = "relationship-schema.csv";

    private final CsvAppender csvAppender;

    public CsvRelationshipSchemaVisitor(Path fileLocation) {
        try {
            this.csvAppender = new CsvWriter().append(fileLocation.resolve(RELATIONSHIP_SCHEMA_FILE_NAME), StandardCharsets.UTF_8);
            writeHeader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void export() {
        try {
            csvAppender.appendField(relationshipType().name());
            csvAppender.appendField(key());
            csvAppender.appendField(valueType().csvName());
            csvAppender.appendField(defaultValue().toString());
            csvAppender.appendField(aggregation().name());
            csvAppender.appendField(state().name());
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
        csvAppender.appendField("label");
        csvAppender.appendField("propertyKey");
        csvAppender.appendField("valueType");
        csvAppender.appendField("defaultValue");
        csvAppender.appendField("aggregation");
        csvAppender.appendField("state");
        csvAppender.endLine();
    }
}
