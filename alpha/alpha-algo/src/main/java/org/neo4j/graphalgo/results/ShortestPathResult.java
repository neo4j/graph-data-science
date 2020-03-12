/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.result.AbstractResultBuilder;

public class ShortestPathResult {

    public final long createMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long nodeCount;
    public final String targetProperty;

    public ShortestPathResult(
        long createMillis,
        long computeMillis,
        long writeMillis,
        long nodeCount,
        String targetProperty
    ) {
        this.createMillis = createMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodeCount = nodeCount;
        this.targetProperty = targetProperty;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ShortestPathResult> {

        private long nodeCount = 0;
        private String targetProperty = "";

        public Builder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        @Override
        public ShortestPathResult build() {
            return new ShortestPathResult(
                createMillis,
                computeMillis,
                writeMillis,
                nodeCount,
                targetProperty
            );
        }
    }

}
