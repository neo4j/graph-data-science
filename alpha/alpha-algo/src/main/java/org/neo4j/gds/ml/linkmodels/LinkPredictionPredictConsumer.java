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
package org.neo4j.gds.ml.linkmodels;

import org.neo4j.gds.ml.Predictor;
import org.neo4j.gds.ml.batch.Batch;
import org.neo4j.gds.ml.batch.BatchTransformer;
import org.neo4j.gds.ml.batch.MappedBatch;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.function.Consumer;

public class LinkPredictionPredictConsumer implements Consumer<Batch> {

    private static final double THRESHOLD = 0.5;
    public static final long PREDICTED_LINK = 1L;
    public static final long PREDICTED_NO_LINK = 0L;

    private final Graph graph;
    private final BatchTransformer nodeIds;
    private final Predictor<double[], LinkLogisticRegressionData> predictor;
    private final HugeDoubleArray predictedProbabilities;
    private final HugeLongArray predictedClasses;
    private final ProgressLogger progressLogger;
    private long index;

    LinkPredictionPredictConsumer(
        Graph graph,
        BatchTransformer nodeIds,
        Predictor<double[], LinkLogisticRegressionData> predictor,
        HugeDoubleArray predictedProbabilities,
        HugeLongArray predictedClasses,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.nodeIds = nodeIds;
        this.predictor = predictor;
        this.predictedProbabilities = predictedProbabilities;
        this.predictedClasses = predictedClasses;
        this.progressLogger = progressLogger;
        this.index = 0L;
    }

    @Override
    public void accept(Batch batch) {
        var originalNodeIdsBatch = new MappedBatch(batch, nodeIds);
        var probabilitiesForBatch = predictor.predict(graph, originalNodeIdsBatch);

        for (double probability : probabilitiesForBatch) {
            predictedProbabilities.set(index, probability);

            if (probability > THRESHOLD) {
                predictedClasses.set(index, PREDICTED_LINK);
            } else {
                predictedClasses.set(index, PREDICTED_NO_LINK);
            }

            index++;
        }

        progressLogger.logProgress(batch.size());
    }
}
