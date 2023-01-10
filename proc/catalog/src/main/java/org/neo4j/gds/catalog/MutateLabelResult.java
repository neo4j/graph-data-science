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

public final class MutateLabelResult {

    public final long mutateMillis;
    public final String nodeLabel;
    public final long nodeLabelsWritten;
    public final long computeMillis;
    public final long nodeCount;
    public final Map<String, Object> configuration;

    private MutateLabelResult(
        long mutateMillis,
        String nodeLabel,
        long nodeLabelsWritten,
        long computeMillis,
        long nodeCount,
        Map<String, Object> configuration
    ) {
        this.mutateMillis = mutateMillis;
        this.nodeLabel = nodeLabel;
        this.nodeLabelsWritten = nodeLabelsWritten;
        this.computeMillis = computeMillis;
        this.nodeCount = nodeCount;
        this.configuration = configuration;
    }

    public static Builder builder(String nodeLabel) {
        return new Builder(nodeLabel);
    }

    public static class Builder extends AbstractResultBuilder<MutateLabelResult> {

        private long nodeLabelsWritten;
        private Map<String, Object> configuration;
        private final String nodeLabel;

        public Builder(String nodeLabel) {this.nodeLabel = nodeLabel;}

        Builder withNodeLabelsWritten(long propertiesWritten) {
            this.nodeLabelsWritten = propertiesWritten;
            return this;
        }

        Builder withConfig(Map<String, Object> configuration) {
            this.configuration = configuration;
            return this;
        }

        public MutateLabelResult build() {
            return new MutateLabelResult(
                mutateMillis,
                nodeLabel,
                nodeLabelsWritten,
                computeMillis,
                nodeCount,
                configuration
            );
        }
    }

}
