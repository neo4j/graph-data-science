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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ValueClass
public interface MetricData<CONFIG extends TrainingConfig> {


    /**
     * Train metrics
     * @return the metric stats for each candidate model on the train set
     */
    List<ModelStats<CONFIG>> train();

    /**
     * Validation metrics
     * @return the metric stats for each candidate model on the validation set
     */
    List<ModelStats<CONFIG>> validation();

    /**
     * Outer train metric
     * @return the metric value for the winner model on outer training set
     */
    double outerTrain();

    /**
     * Test metric
     * @return the metric value for the winner model on test set (holdout)
     */
    double test();

    @Value.Derived
    default Map<String, Object> toMap() {
        return Map.of(
            "outerTrain", outerTrain(),
            "test", test(),
            "validation", validation().stream().map(ModelStats::toMap).collect(Collectors.toList()),
            "train", train().stream().map(ModelStats::toMap).collect(Collectors.toList())
        );
    }

    static <CONFIG extends TrainingConfig> MetricData<CONFIG> of(List<ModelStats<CONFIG>> train, List<ModelStats<CONFIG>> validation, double outerTrain, double test) {
        return ImmutableMetricData.of(train, validation, outerTrain, test);
    }
}

