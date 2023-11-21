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
package org.neo4j.gds.similarity.knn;

public final class K {
    static K create(int value, long nodeCount, double sampleRate, double deltaThreshold) {
        if (value < 1) throw new IllegalArgumentException("K value must be 1 or more");
        if (Double.compare(sampleRate, 0.0) < 1 || Double.compare(sampleRate, 1.0) > 0)
            throw new IllegalArgumentException("sampleRate must be more than 0.0 and less than or equal to 1.0");
        if (Double.compare(deltaThreshold, 0.0) < 0 || Double.compare(deltaThreshold, 1.0) > 0)
            throw new IllegalArgumentException("deltaThreshold must be more than or equal to 0.0 and less than or equal to 1.0");
        // (int) is safe because value is at most `topK`, which is an int
        var boundedValue = Math.max(0, (int) Math.min(value, nodeCount - 1));
        var sampledValue = Math.max(0, (int) Math.min((long) Math.ceil(sampleRate * value), nodeCount - 1));

        var maxUpdates = (long) Math.ceil(sampleRate * value * nodeCount);
        var updateThreshold = (long) Math.floor(deltaThreshold * maxUpdates);

        return new K(value, boundedValue, sampledValue, updateThreshold);
    }

    public final int value;
    public final int boundedValue;
    public final int sampledValue;
    public final long updateThreshold;

    private K(int value, int boundedValue, int sampledValue, long updateThreshold) {
        this.value = value;
        this.boundedValue = boundedValue;
        this.sampledValue = sampledValue;
        this.updateThreshold = updateThreshold;
    }
}
