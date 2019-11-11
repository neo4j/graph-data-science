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

public class PageRankScore {

    public final long nodeId;
    public final Double score;

    public PageRankScore(long nodeId, final Double score) {
        this.nodeId = nodeId;
        this.score = score;
    }

    // TODO: return number of relationships as well
    //  the Graph API doesn't expose this value yet
    public static final class Stats {
        public final long nodes, iterations, loadMillis, computeMillis, writeMillis;
        public final double dampingFactor;
        public final boolean write;
        public final String writeProperty;

        Stats(
                long nodes,
                long iterations,
                long loadMillis,
                long computeMillis,
                long writeMillis,
                double dampingFactor,
                boolean write,
                String writeProperty) {
            this.nodes = nodes;
            this.iterations = iterations;
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.dampingFactor = dampingFactor;
            this.write = write;
            this.writeProperty = writeProperty;
        }

        public static final class Builder extends AbstractWriteBuilder<Stats> {
            private long nodes;
            private long iterations;
            private double dampingFactor;
            private boolean write;
            private String writeProperty;

            public Builder withNodes(long nodes) {
                this.nodes = nodes;
                return this;
            }

            public Builder withIterations(long iterations) {
                this.iterations = iterations;
                return this;
            }

            public Builder withDampingFactor(double dampingFactor) {
                this.dampingFactor = dampingFactor;
                return this;
            }

            @Override
            public Builder withWrite(boolean write) {
                this.write = write;
                return this;
            }

            @Override
            public Builder withProperty(String writeProperty) {
                this.writeProperty = writeProperty;
                return this;
            }

            public PageRankScore.Stats build() {
                return new PageRankScore.Stats(
                        nodes,
                        iterations,
                        loadDuration,
                        evalDuration,
                        writeDuration,
                        dampingFactor,
                        write,
                        writeProperty);
            }
        }
    }
}
