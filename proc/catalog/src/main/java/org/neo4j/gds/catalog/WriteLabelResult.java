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

public class WriteLabelResult {
    public final long writeMillis;
    public final String graphName;
    public final String nodeLabel;
    public final long nodeLabelsWritten;

    WriteLabelResult(long writeMillis, String graphName, String nodeLabel, long nodeLabelsWritten) {
        this.writeMillis = writeMillis;
        this.graphName = graphName;
        this.nodeLabel = nodeLabel;
        this.nodeLabelsWritten = nodeLabelsWritten;
    }

    static class Builder {
        private final String graphName;
        private final String nodeLabel;
        private long nodeLabelsWritten;
        private long writeMillis;

        Builder(String graphName, String nodeLabel) {
            this.graphName = graphName;
            this.nodeLabel = nodeLabel;
        }

        Builder withWriteMillis(long writeMillis) {
            this.writeMillis = writeMillis;
            return this;
        }

        Builder withNodeLabelsWritten(long propertiesWritten) {
            this.nodeLabelsWritten = propertiesWritten;
            return this;
        }

        WriteLabelResult build() {
            return new WriteLabelResult(writeMillis, graphName, nodeLabel, nodeLabelsWritten);
        }
    }
}
