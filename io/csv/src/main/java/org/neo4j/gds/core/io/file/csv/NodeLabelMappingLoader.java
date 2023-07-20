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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.neo4j.gds.core.io.NodeLabelInverseMapping;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

public class NodeLabelMappingLoader {

    private final ObjectReader objectReader;
    private final Path labelMappingPath;
    private final NodeLabelInverseMapping mapping;

    NodeLabelMappingLoader(Path csvDirectory) {
        this.mapping = new NodeLabelInverseMapping();
        this.labelMappingPath = csvDirectory.resolve(CsvNodeLabelMappingVisitor.LABEL_MAPPING_FILE_NAME);
        CsvMapper csvMapper = new CsvMapper();
        csvMapper.enable(CsvParser.Feature.TRIM_SPACES);
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        this.objectReader = csvMapper.readerFor(MappingLine.class).with(schema);
    }

    Optional<NodeLabelInverseMapping> load() {
        var file = labelMappingPath.toFile();
        if (! file.isFile()) {
            return Optional.empty();
        }

        try(var reader = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            var linesIterator = objectReader.<MappingLine>readValues(reader);
            while(linesIterator.hasNext()) {
                var mappingLine = linesIterator.next();
                mapping.add(mappingLine.index, mappingLine.label);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Optional.of(mapping);
    }

    public static class MappingLine {
        @JsonProperty
        String index;

        @JsonProperty
        String label;
    }
}
