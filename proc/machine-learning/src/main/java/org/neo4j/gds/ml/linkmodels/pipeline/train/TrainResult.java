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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.training.TrainingStatistics;

import java.util.Map;
import java.util.Optional;

public class TrainResult extends MLTrainResult {

    public final Map<String, Object> modelSelectionStats;

    TrainResult(
        Model<Classifier.ClassifierData, LinkPredictionTrainConfig, LinkPredictionModelInfo> model,
        TrainingStatistics trainingStatistics,
        long trainMillis
    ) {
        super(Optional.of(model), trainMillis);
        this.modelSelectionStats = trainingStatistics.toMap();;
    }
}
