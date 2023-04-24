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
package org.neo4j.gds.triangle;

import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

@SuppressWarnings("unused")
public class TriangleCountWriteResult extends TriangleCountStatsResult {

    public long writeMillis;
    public long nodePropertiesWritten;

    public TriangleCountWriteResult(
        long globalTriangleCount,
        long nodeCount,
        long preProcessingMillis,
        long computeMillis,
        long writeMillis,
        long nodePropertiesWritten,
        Map<String, Object> configuration
    ) {
        super(
            globalTriangleCount,
            nodeCount,
            preProcessingMillis,
            computeMillis,
            configuration
        );
        this.writeMillis = writeMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    static class Builder extends AbstractResultBuilder<TriangleCountWriteResult> {

        long globalTriangleCount = 0;

        Builder withGlobalTriangleCount(long globalTriangleCount) {
            this.globalTriangleCount = globalTriangleCount;
            return this;
        }

        @Override
        public TriangleCountWriteResult build() {
            return new TriangleCountWriteResult(
                globalTriangleCount,
                nodeCount,
                preProcessingMillis,
                computeMillis,
                writeMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
