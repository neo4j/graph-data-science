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
package org.neo4j.gds.compat;

import org.immutables.builder.Builder;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;

public final class LogBuilders {

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ", Locale.ENGLISH);

    @Builder.Factory
    public static Log outputStreamLog(
        @Builder.Parameter OutputStream outputStream,
        Optional<Level> level,
        Optional<ZoneId> zoneId,
        Optional<DateTimeFormatter> dateTimeFormatter,
        Optional<String> category
    ) {
        return Neo4jProxy.logger(
            level.orElse(Level.INFO),
            zoneId.orElse(ZoneOffset.UTC),
            dateTimeFormatter.orElse(DATE_TIME_FORMATTER),
            category.orElse(null),
            outputStream
        );
    }

    @Builder.Factory
    public static Log writerLog(
        @Builder.Parameter Writer writer,
        Optional<Level> level,
        Optional<ZoneId> zoneId,
        Optional<DateTimeFormatter> dateTimeFormatter,
        Optional<String> category
    ) {
        return Neo4jProxy.logger(
            level.orElse(Level.INFO),
            zoneId.orElse(ZoneOffset.UTC),
            dateTimeFormatter.orElse(DATE_TIME_FORMATTER),
            category.orElse(null),
            writer instanceof PrintWriter
                ? (PrintWriter) writer
                : new PrintWriter(writer)
        );
    }

    private LogBuilders() {
        throw new UnsupportedOperationException("No instances");
    }
}
