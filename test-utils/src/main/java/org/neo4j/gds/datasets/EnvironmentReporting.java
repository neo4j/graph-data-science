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
package org.neo4j.gds.datasets;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;

public final class EnvironmentReporting {
    private EnvironmentReporting() {}

    public static String diskUsage() throws IOException {
        Process p = new ProcessBuilder("df", "-h").start();
        String stdout = IOUtils.toString(p.getInputStream(), Charset.defaultCharset());
        return "$ df -h\r" + asMultilineCloudWatchLogMessage(stdout);
    }

    public static String directoryContents(Path directory) throws IOException {
        Process p = new ProcessBuilder("ls", "-l", directory.toAbsolutePath().toString()).start();
        String stdout = IOUtils.toString(p.getInputStream(), Charset.defaultCharset());
        return "$ ls -l " + directory + "\r" + asMultilineCloudWatchLogMessage(stdout);
    }

    /**
     * https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/AgentReference.html, multi_line_start_pattern
     */
    @NotNull
    private static String asMultilineCloudWatchLogMessage(String logMessage) {
        return logMessage.replace('\n', '\r');
    }
}
