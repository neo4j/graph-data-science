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
package org.neo4j.gds.ml.pipeline.node.classification;

import org.neo4j.gds.ml_api.TrainingMethod;

import java.util.List;
import java.util.Map;

public final class NodeClassificationPipelineCompanion {
    public static final String PREDICT_DESCRIPTION = "Predicts classes for all nodes based on a previously trained pipeline model";
    public static final String ESTIMATE_PREDICT_DESCRIPTION = "Estimates memory for predicting classes for all nodes based on a previously trained pipeline model";
    static final Map<String, Object> DEFAULT_SPLIT_CONFIG =  Map.of("testFraction", 0.3, "validationFolds", 3);
    static final Map<String, List<Map<String, Object>>> DEFAULT_PARAM_CONFIG = Map.of(
        TrainingMethod.LogisticRegression.toString(), List.of(),
        TrainingMethod.RandomForestClassification.toString(), List.of(),
        TrainingMethod.MLPClassification.toString(), List.of()
    );

    private NodeClassificationPipelineCompanion() {}
}
