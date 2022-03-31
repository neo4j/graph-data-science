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
package org.neo4j.gds.core.utils.io.db;

import org.eclipse.collections.impl.list.mutable.primitive.ByteArrayList;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class LoggingOutputStream extends OutputStream {

    private final Log log;
    private final ByteArrayList buffer;

    public LoggingOutputStream(Log log) {
        this.log = log;
        this.buffer = new ByteArrayList();
    }

    @Override
    public void write(int b) throws IOException {
        var nextByte = (byte) (b & 0xff);

        if ((char) nextByte == '\n') {
            flush ();
        } else {
            buffer.add(nextByte);
        }
    }

    public void flush () {
        var message = new String(buffer.toArray(), StandardCharsets.UTF_8);
        log.debug(message);
        buffer.clear();
    }
}
