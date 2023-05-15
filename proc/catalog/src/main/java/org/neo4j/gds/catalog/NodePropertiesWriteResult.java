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
package org.neo4j.gds.catalog;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public final class NodePropertiesWriteResult {
    public final long writeMillis;
    public final String graphName;
    public final List<String> nodeProperties;
    public final long propertiesWritten;

    private NodePropertiesWriteResult(
        long writeMillis,
        String graphName,
        Collection<String> nodeProperties,
        long propertiesWritten
    ) {
        this.writeMillis = writeMillis;
        this.graphName = graphName;
        this.nodeProperties = nodeProperties.stream().sorted().collect(Collectors.toList());
        this.propertiesWritten = propertiesWritten;
    }

    static class Builder {
        private final String graphName;
        private final List<String> nodeProperties;
        private long propertiesWritten;
        private long writeMillis;

        Builder(String graphName, List<String> nodeProperties) {
            this.graphName = graphName;
            this.nodeProperties = nodeProperties;
        }

        Builder withWriteMillis(long writeMillis) {
            this.writeMillis = writeMillis;
            return this;
        }

        Builder withPropertiesWritten(long propertiesWritten) {
            this.propertiesWritten = propertiesWritten;
            return this;
        }

        NodePropertiesWriteResult build() {
            return new NodePropertiesWriteResult(writeMillis, graphName, nodeProperties, propertiesWritten);
        }
    }
}
