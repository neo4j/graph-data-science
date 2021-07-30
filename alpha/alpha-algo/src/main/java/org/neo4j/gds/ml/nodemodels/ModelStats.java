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
package org.neo4j.gds.ml.nodemodels;

import org.immutables.value.Value;
import org.neo4j.gds.ml.TrainingConfig;
import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.Comparator;
import java.util.Map;

@ValueClass
public interface ModelStats<CONFIG extends TrainingConfig> {
    Comparator<ModelStats<?>> COMPARE_AVERAGE = Comparator.comparingDouble(ModelStats::avg);

    /**
     * The input params representing a model candidate
     * @return
     */
    CONFIG params();

    /**
     * The average of the metric of the model candidate over (inner) folds
     * @return
     */
    double avg();
    /**
     * The minimum of the metric of the model candidate over (inner) folds
     * @return
     */
    double min();
    /**
     * The maximum of the metric of the model candidate over (inner) folds
     * @return
     */
    double max();

    @Value.Derived
    default Map<String, Object> toMap() {
        return Map.of(
            "avg", avg(),
            "min", min(),
            "max", max(),
            "params", params().toMap()
        );
    }
}
