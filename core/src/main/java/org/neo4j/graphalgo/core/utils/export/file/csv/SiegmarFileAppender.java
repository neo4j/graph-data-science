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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

class SiegmarFileAppender implements FileAppender {

    private final CsvAppender csvAppender;

    SiegmarFileAppender(Path filePath) {
        try {
            csvAppender = new CsvWriter().append(filePath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void append(long value) throws IOException {
        csvAppender.appendField(Long.toString(value));
    }

    @Override
    public void append(String value) throws IOException {
        csvAppender.appendField(value);
    }

    @Override
    public void endLine() throws IOException {
        csvAppender.endLine();
    }

    @Override
    public void flush() throws IOException {
        csvAppender.flush();
    }

    @Override
    public void close() throws IOException {
        csvAppender.close();
    }
}
