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
package org.neo4j.gds.models.logisticregression;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.models.Classifier;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.Optional;

@ValueClass
public interface LogisticRegressionData extends Classifier.ClassifierData {

    Weights<Matrix> weights();
    Optional<Weights<Vector>> bias();
    LocalIdMap classIdMap();

    static LogisticRegressionData standard(int featureCount, boolean useBias, LocalIdMap classIdMap) {
        return create(classIdMap.size(), featureCount, useBias, classIdMap);
    }

    // this is an optimization where "virtually" add a weight of 0.0 for the last class
    static LogisticRegressionData withReducedClassCount(int featureCount, boolean useBias, LocalIdMap classIdMap) {
        return create(classIdMap.size() - 1, featureCount, useBias, classIdMap);
    }

    private static LogisticRegressionData create(int classCount, int featureCount, boolean useBias, LocalIdMap classIdMap) {
        var weights = Weights.ofMatrix(classCount, featureCount);

        var bias = useBias
            ? Optional.of(new Weights<>(new Vector(classCount)))
            : Optional.<Weights<Vector>>empty();

        return ImmutableLogisticRegressionData.builder().weights(weights).classIdMap(classIdMap).bias(bias).build();
    }

    static LogisticRegressionData create(Weights<Matrix> weights, Optional<Weights<Vector>> bias, LocalIdMap classIdMap) {
        return ImmutableLogisticRegressionData.builder().bias(bias).weights(weights).classIdMap(classIdMap).build();
    }

    static MemoryEstimation memoryEstimationBinaryReduced(MemoryRange featureDimension) {
        return MemoryEstimations.builder(LogisticRegressionData.class)
            .fixed("weights", featureDimension.apply(featureDim -> Weights.sizeInBytes(
                1,
                Math.toIntExact(featureDim)
            )))
            .fixed("bias", Weights.sizeInBytes(1, 1))
            .build();
    }

    static MemoryEstimation memoryEstimation(int numberOfClasses, int numberOfFeatures, boolean useBias) {
        var builder = MemoryEstimations.builder(LogisticRegressionData.class)
            .add("classIdMap", LocalIdMap.memoryEstimation(numberOfClasses))
            .fixed("weights", Weights.sizeInBytes(numberOfClasses, numberOfFeatures));
        if (useBias) {
            builder.fixed("bias", Weights.sizeInBytes(numberOfClasses, 1));
        }
        return builder.build();
    }

    static MemoryEstimation memoryEstimation(int numberOfClasses, int numberOfFeatures) {
        return MemoryEstimations.builder(LogisticRegressionData.class)
            .add("classIdMap", LocalIdMap.memoryEstimation(numberOfClasses))
            .fixed("weights", Weights.sizeInBytes(numberOfClasses, numberOfFeatures))
            .build();
    }


    static ImmutableLogisticRegressionData.Builder builder() {
        return ImmutableLogisticRegressionData.builder();
    }
}
