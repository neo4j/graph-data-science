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

import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

public class WriteLabelResult {
    public final long writeMillis;
    public final String graphName;
    public final String nodeLabel;
    public final long nodeCount;
    public final long nodeLabelsWritten;
    public final Map<String, Object> configuration;


    WriteLabelResult(
        long writeMillis,
        String graphName,
        String nodeLabel,
        long nodeLabelsWritten,
        long nodeCount,
        Map<String, Object> configuration
    ) {
        this.writeMillis = writeMillis;
        this.graphName = graphName;
        this.nodeLabel = nodeLabel;
        this.nodeLabelsWritten = nodeLabelsWritten;
        this.nodeCount = nodeCount;
        this.configuration = configuration;
    }

    public static Builder builder(String graphName, String nodeLabel) {
        return new Builder(graphName, nodeLabel);
    }


    static class Builder extends AbstractResultBuilder<WriteLabelResult> {
        private final String graphName;
        private final String nodeLabel;
        private long nodeLabelsWritten;
        private Map<String, Object> configuration;

        Builder(String graphName, String nodeLabel) {
            this.graphName = graphName;
            this.nodeLabel = nodeLabel;
        }

        Builder withNodeLabelsWritten(long propertiesWritten) {
            this.nodeLabelsWritten = propertiesWritten;
            return this;
        }

        Builder withConfig(Map<String, Object> configuration) {
            this.configuration = configuration;
            return this;
        }

        public WriteLabelResult build() {
            return new WriteLabelResult(
                writeMillis,
                graphName,
                nodeLabel,
                nodeLabelsWritten,
                nodeCount,
                configuration
            );
        }
    }
}
