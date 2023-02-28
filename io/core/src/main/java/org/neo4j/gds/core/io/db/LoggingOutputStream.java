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
package org.neo4j.gds.core.io.db;

import org.neo4j.logging.Log;

import java.io.IOException;
import java.io.OutputStream;

public class LoggingOutputStream extends OutputStream {

    private final Log log;
    private final int lineSeparatorLength;
    private StringBuffer buffer;

    public LoggingOutputStream(Log log) {
        this.log = log;
        this.lineSeparatorLength = System.lineSeparator().length();
        this.buffer = new StringBuffer();
    }

    @Override
    public void write(int b) throws IOException {
        var nextByte = (char) (b & 0xff);

        buffer.append(nextByte);
        var lineSeparatorStartLocation = buffer.length() - lineSeparatorLength;
        if (buffer.substring(lineSeparatorStartLocation).equals(System.lineSeparator())) {
            buffer.delete(lineSeparatorStartLocation, buffer.length());
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (!buffer.isEmpty()) {
            flush();
        }
        super.close();
    }

    public void flush() {
        if (!buffer.isEmpty()) {
            log.debug(buffer.toString());
            buffer = new StringBuffer();
        }
    }
}
