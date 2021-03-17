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
package org.neo4j.graphalgo.core.utils.export.file;

import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.Set;

@ValueClass
public interface NodeFileHeader {
    Set<HeaderProperty> propertyMappings();
    String[] nodeLabels();

    static NodeFileHeaderBuilder builder() {
        return new NodeFileHeaderBuilder();
    }

    final class NodeFileHeaderBuilder {
        private String headerLine;
        private String[] nodeLabels;
        private NodeFileHeaderBuilder() {}

        NodeFileHeaderBuilder withHeaderLine(String headerLine) {
            this.headerLine = headerLine;
            return this;
        }

        NodeFileHeaderBuilder withNodeLabels(String[] nodeLabels) {
            this.nodeLabels = nodeLabels;
            return this;
        }

        NodeFileHeader build() {
            var builder = ImmutableNodeFileHeader.builder();
            String[] csvColumns = headerLine.split(",");
            if (csvColumns.length > 1) {
                for (int i = 1; i < csvColumns.length; i++) {
                    builder.addPropertyMapping(HeaderProperty.parse(i, csvColumns[i]));
                }
            }
            builder.nodeLabels(nodeLabels);
            return builder.build();
        }
    }
}
