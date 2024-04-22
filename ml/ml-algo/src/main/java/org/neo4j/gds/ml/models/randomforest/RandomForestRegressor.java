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
package org.neo4j.gds.ml.models.randomforest;

import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredictor;
import org.neo4j.gds.ml.models.Regressor;

import java.util.List;

import static org.neo4j.gds.mem.Estimate.sizeOfInstance;

public class RandomForestRegressor implements Regressor {

    private final RandomForestRegressorData data;

    public RandomForestRegressor(
        List<DecisionTreePredictor<Double>> decisionTrees,
        int featureDimension
    ) {
        this(ImmutableRandomForestRegressorData.of(featureDimension, decisionTrees));
    }

    public RandomForestRegressor(RandomForestRegressorData data) {
        this.data = data;
    }

    public static MemoryRange runtimeOverheadMemoryEstimation() {
        return MemoryRange.of(sizeOfInstance(RandomForestRegressor.class));
    }

    @Override
    public RegressorData data() {
        return data;
    }

    @Override
    public double predict(double[] features) {
        int numberOfDecisionTrees = data.decisionTrees().size();

        double sum = 0;
        for (int i = 0; i < numberOfDecisionTrees; i++) {
            sum += data.decisionTrees().get(i).predict(features);
        }

        return sum / numberOfDecisionTrees;
    }
}
