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
import org.neo4j.gds.ml.nodemodels.metrics.Metric;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrainConfig;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.model.Model;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ValueClass
public interface NodeClassificationModelInfo extends Model.Mappable {

    /**
     * The distinct values of the target property which represent the
     * allowed classes that the model can predict.
     * @return
     */
    List<Long> classes();

    /**
     * The parameters that yielded the best fold-averaged validation score
     * for the selection metric.
     * @return
     */
    MultiClassNLRTrainConfig bestParameters();
    Map<Metric, MetricData<MultiClassNLRTrainConfig>> metrics();

    @Override
    @Value.Auxiliary
    @Value.Derived
    default Map<String, Object> toMap() {
        return Map.of(
            "bestParameters", bestParameters().toMap(),
            "classes", classes(),
            "metrics", metrics().entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> entry.getValue().toMap()
            ))
        );
    }

    static NodeClassificationModelInfo of(
        List<Long> classes,
        MultiClassNLRTrainConfig bestParameters,
        Map<Metric, MetricData<MultiClassNLRTrainConfig>> metrics
    ) {
        return ImmutableNodeClassificationModelInfo.of(classes, bestParameters, metrics);
    }
}

