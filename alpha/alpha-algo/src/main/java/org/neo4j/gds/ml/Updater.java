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
package org.neo4j.gds.ml;

import org.neo4j.gds.core.ml.ComputationContext;
import org.neo4j.gds.core.ml.functions.Weights;
import org.neo4j.gds.core.ml.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.AdamOptimizer;

import java.util.List;

public interface Updater {
    void update(ComputationContext ctx);

    static long sizeInBytesOfDefaultUpdater(int rows, int cols, int numberOfWeights) {
        return AdamOptimizer.sizeInBytes(rows, cols, numberOfWeights);
    }

    static Updater defaultUpdater(List<Weights<? extends Tensor<?>>> weights) {
        AdamOptimizer adamOptimizer = new AdamOptimizer(weights);
        return adamOptimizer::update;
    }
}
