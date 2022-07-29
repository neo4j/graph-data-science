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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class LoggingOutputStreamTest {

    @Test
    void shouldLog() {
        var log = Neo4jProxy.testLog();
        var loggingOutputStream = new LoggingOutputStream(log);

        var testString = "hello world" + System.lineSeparator() + "with new line";
        try (var writer = new PrintWriter(loggingOutputStream, false, StandardCharsets.UTF_8)) {
            writer.println(testString);
        }

        assertThat(log.getMessages(TestLog.DEBUG)).contains(testString.split(System.lineSeparator()));
    }

}
