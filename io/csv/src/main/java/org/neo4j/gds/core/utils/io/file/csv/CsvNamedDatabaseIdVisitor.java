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
import org.neo4j.gds.core.io.file.SingleRowVisitor;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class CsvNamedDatabaseIdVisitor implements SingleRowVisitor<NamedDatabaseId> {

    static final String DATABASE_ID_FILE_NAME = "database_id.csv";
    static final String DATABASE_ID_COLUMN_NAME = "databaseId";

    private final CsvAppender csvAppender;

    public CsvNamedDatabaseIdVisitor(Path fileLocation) {
        try {
            this.csvAppender = new CsvWriter().append(fileLocation.resolve(DATABASE_ID_FILE_NAME), StandardCharsets.UTF_8);
            writeHeader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void export(NamedDatabaseId namedDatabaseId) {
        try {
            this.csvAppender.appendField(namedDatabaseId.toString());
            this.csvAppender.endLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            this.csvAppender.flush();
            this.csvAppender.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeHeader() throws IOException {
        this.csvAppender.appendField(DATABASE_ID_COLUMN_NAME);
        this.csvAppender.endLine();
    }
}
