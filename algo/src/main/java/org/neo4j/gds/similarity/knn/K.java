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

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class K {
    public static K create(int k, long nodeCount, double sampleRate, double deltaThreshold) {
        // user-provided k value must be at least 1
        if (k < 1) throw new IllegalArgumentException("K k must be 1 or more");
        // sampleRate -- value range (0.0;1.0]
        if (Double.compare(sampleRate, 0.0) < 1 || Double.compare(sampleRate, 1.0) > 0)
            throw new IllegalArgumentException("sampleRate must be more than 0.0 and less than or equal to 1.0");
        // deltaThreshold -- value range [0.0;1.0]
        if (Double.compare(deltaThreshold, 0.0) < 0 || Double.compare(deltaThreshold, 1.0) > 0)
            throw new IllegalArgumentException(formatWithLocale("deltaThreshold must be more than or equal to 0.0 and less than or equal to 1.0, was `%f`", deltaThreshold));

        // (int) is safe because k is at most `topK`, which is an int
        // upper bound for k is all other nodes in the graph
        var boundedValue = Math.max(0, (int) Math.min(k, nodeCount - 1));
        var sampledValue = Math.max(0, (int) Math.min((long) Math.ceil(sampleRate * k), nodeCount - 1));

        var maxUpdates = (long) Math.ceil(sampleRate * k * nodeCount);
        var updateThreshold = (long) Math.floor(deltaThreshold * maxUpdates);

        return new K(boundedValue, sampledValue, updateThreshold);
    }

    public final int value;
    public final int sampledValue;
    public final long updateThreshold;

    private K(int value, int sampledValue, long updateThreshold) {
        this.value = value;
        this.sampledValue = sampledValue;
        this.updateThreshold = updateThreshold;
    }
}
