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
package org.neo4j.gds.ml.models.linearregression;

import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.models.Regressor;

class LinearRegressor implements Regressor {
    private final LinearRegressionData data;

    LinearRegressor(LinearRegressionData data) {this.data = data;}

    @Override
    public double predict(double[] features) {
        Vector weights = this.data.weights().data();

        double prediction = 0;

        for (int i = 0; i < weights.length(); i++) {
            prediction += weights.dataAt(i) * features[i];
        }

        return prediction + this.data.bias().data().value();
    }

    @Override
    public LinearRegressionData data() {
        return data;
    }
}
