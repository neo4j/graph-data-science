/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

/**
 * @author mknblch
 */
public class ShortestPathResult {

    public final long loadDuration;
    public final long evalDuration;
    public final long writeDuration;
    public final long nodeCount;
    public final String targetProperty;

    public ShortestPathResult(long loadDuration,
                              long evalDuration,
                              long writeDuration,
                              long nodeCount,
                              String targetProperty) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
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

        public Builder withTargetProperty(String targetProperty) {
            this.targetProperty = targetProperty;
            return this;
        }

        @Override
        public ShortestPathResult build() {
            return new ShortestPathResult(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    nodeCount,
                    targetProperty);
        }
    }

}
