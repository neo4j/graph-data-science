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
import org.neo4j.gds.core.io.schema.RelationshipSchemaVisitor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.neo4j.gds.core.io.file.csv.CsvNodeSchemaVisitor.DEFAULT_VALUE_COLUMN_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvNodeSchemaVisitor.PROPERTY_KEY_COLUMN_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvNodeSchemaVisitor.STATE_COLUMN_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvNodeSchemaVisitor.VALUE_TYPE_COLUMN_NAME;

public class CsvRelationshipSchemaVisitor extends RelationshipSchemaVisitor {

    static final String RELATIONSHIP_TYPE_COLUMN_NAME = "relationshipType";
    static final String AGGREGATION_COLUMN_NAME = "aggregation";

    static final String DIRECTION_COLUMN_NAME = "direction";

    public static final String RELATIONSHIP_SCHEMA_FILE_NAME = "relationship-schema.csv";

    private final CsvWriter csvWriter;

    public CsvRelationshipSchemaVisitor(Path fileLocation) {
        try {
            this.csvWriter = CsvWriter.builder().build(fileLocation.resolve(RELATIONSHIP_SCHEMA_FILE_NAME), StandardCharsets.UTF_8);
            writeHeader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void export() {
        var row = new ArrayList<String>();

        row.add(relationshipType().name());
        row.add(direction().name());
        if (key() != null) {
            row.add(key());
            row.add(valueType().csvName());
            row.add(defaultValue().toString());
            row.add(aggregation().name());
            row.add(state().name());
        }
        csvWriter.writeRow(row);
    }

    @Override
    public void close() {
        try {
            csvWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeHeader() {
        csvWriter.writeRow(
            RELATIONSHIP_TYPE_COLUMN_NAME,
            DIRECTION_COLUMN_NAME,
            PROPERTY_KEY_COLUMN_NAME,
            VALUE_TYPE_COLUMN_NAME,
            DEFAULT_VALUE_COLUMN_NAME,
            AGGREGATION_COLUMN_NAME,
            STATE_COLUMN_NAME
        );
    }
}
