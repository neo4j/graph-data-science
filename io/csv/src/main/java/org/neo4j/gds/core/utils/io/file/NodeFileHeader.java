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
package org.neo4j.gds.core.utils.io.file;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.io.file.FileHeader;
import org.neo4j.gds.core.io.file.HeaderProperty;
import org.neo4j.gds.core.utils.io.file.csv.CsvNodeVisitor;
import org.neo4j.gds.utils.StringFormatting;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ValueClass
public interface NodeFileHeader extends FileHeader<NodeSchema, NodeLabel, PropertySchema> {
    Set<HeaderProperty> propertyMappings();
    String[] nodeLabels();

    @Override
    default Map<String, PropertySchema> schemaForIdentifier(NodeSchema schema) {
        var labelStream = Arrays.stream(nodeLabels()).map(NodeLabel::of);
        if (nodeLabels().length == 0) {
            labelStream = Stream.of(NodeLabel.ALL_NODES);
        }
        Set<NodeLabel> nodeLabels = labelStream.collect(Collectors.toSet());
        return schema.filter(nodeLabels).unionProperties();
    }

    static NodeFileHeader of(String headerLine, String[] nodeLabels) {
        var builder = ImmutableNodeFileHeader.builder();
        String[] csvColumns = headerLine.split(",");
        if (csvColumns.length == 0 || !csvColumns[0].equals(CsvNodeVisitor.ID_COLUMN_NAME)) {
            throw new IllegalArgumentException(StringFormatting.formatWithLocale("First column of header must be %s.", CsvNodeVisitor.ID_COLUMN_NAME));
        }
        for (int i = 1; i < csvColumns.length; i++) {
            builder.addPropertyMapping(HeaderProperty.parse(i, csvColumns[i]));
        }
        builder.nodeLabels(nodeLabels);
        return builder.build();
    }
}
