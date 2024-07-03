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
package org.neo4j.gds.procedures.algorithms.embeddings;

import java.util.ArrayList;
import java.util.List;

public final class FastRPStreamResult {
    public final long nodeId;
    public final List<Double> embedding;

    private FastRPStreamResult(long nodeId, List<Double> embedding) {
        this.nodeId = nodeId;
        this.embedding = embedding;
    }

    static FastRPStreamResult create(long nodeId, float[] embeddingAsArray) {
        var embedding = new ArrayList<Double>(embeddingAsArray.length);
        for (var f : embeddingAsArray) {
            embedding.add((double) f);
        }
        return new FastRPStreamResult(nodeId, embedding);
    }
}
