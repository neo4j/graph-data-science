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
package org.neo4j.gds.models;

import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;

public interface Classifier {
    default int numberOfClasses() {
        return classIdMap().size();
    }

    LocalIdMap classIdMap();

    double[] predictProbabilities(long id, Features features);
    default Matrix predictProbabilities(Batch batch, Features features) {
        double[] predictedProbabilities = new double[batch.size() * numberOfClasses()];
        var offset = 0;
        for (long id : batch.nodeIds()) {
            var predictionsForNode = predictProbabilities(id, features);
            System.arraycopy(predictionsForNode, 0, predictedProbabilities, offset * numberOfClasses(), numberOfClasses());
            offset++;
        }
        return new Matrix(predictedProbabilities, batch.size(), numberOfClasses());
    }

    ClassifierData data();

    // placeholder
    interface ClassifierData {
    }
}
