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
package org.neo4j.gds.k1coloring;

public final class K1ColoringParameters {

    static K1ColoringParameters create(int concurrency, int maxIterations, int batchSize) {
        return new K1ColoringParameters(concurrency, maxIterations, batchSize);
    }

    private final int concurrency;
    private final int maxIterations;
    private final int batchSize;


    private K1ColoringParameters(int concurrency, int maxIterations, int batchSize) {
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        this.batchSize = batchSize;
    }

    int concurrency() {
        return concurrency;
    }

    int maxIterations() {
        return maxIterations;
    }

    int batchSize() {
        return batchSize;
    }
}
