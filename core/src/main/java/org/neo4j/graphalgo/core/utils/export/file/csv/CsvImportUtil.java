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

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.neo4j.graphalgo.core.utils.export.file.NodeFileHeader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CsvImportUtil {

    private CsvImportUtil() {}

    public static NodeFileHeader parseHeader(Path headerFile) {
        try (var headerReader = new BufferedReader(new FileReader(headerFile.toFile(), StandardCharsets.UTF_8))) {
            return NodeFileHeader.builder()
                .withHeaderLine(headerReader.readLine())
                .withNodeLabels(inferNodeLabels(headerFile))
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<Path, List<Path>> headerToFileMapping(Path csvDirectory) {
        Map<Path, List<Path>> headerToDataFileMapping = new HashMap<>();
        for (File nodeHeaderFile : getNodeHeaderFiles(csvDirectory)) {
            String nodeDataFilePattern = nodeHeaderFile.getName().replace("_header", "(_\\d+)");
            List<Path> nodeDataPaths = headerToDataFileMapping.computeIfAbsent(
                nodeHeaderFile.toPath(),
                path -> new ArrayList<>()
            );
            for (File nodeDataFile : getFilesByRegex(csvDirectory, nodeDataFilePattern)) {
                nodeDataPaths.add(nodeDataFile.toPath());
            }
        }
        return headerToDataFileMapping;
    }

    static File[] getNodeHeaderFiles(Path csvDirectory) {
        String nodeFilesPattern = "^nodes(_\\w)+_header.csv";
        return getFilesByRegex(csvDirectory, nodeFilesPattern);
    }

    private static File[] getFilesByRegex(Path csvDirectory, String pattern) {
        FilenameFilter fileNameFilter = new RegexFileFilter(pattern);
        return csvDirectory.toFile().listFiles(fileNameFilter);
    }

    private static String[] inferNodeLabels(Path headerFile) {
        var headerFileName = headerFile.toFile().getName();
        return headerFileName.replaceAll("nodes_|_header.csv", "").split("_");
    }
}
