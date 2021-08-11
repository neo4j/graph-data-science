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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionPredictor;

import java.util.function.Consumer;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class SignedProbabilitiesCollector implements Consumer<Batch> {

    private static final int POSITIVE_LINK = 1;
    private static final int NEGATIVE_LINK = 0;

    private final Graph graph;
    private final LinkLogisticRegressionPredictor predictor;
    private final SignedProbabilities signedProbabilities;
    private final ProgressTracker progressTracker;

    SignedProbabilitiesCollector(
        Graph graph,
        LinkLogisticRegressionPredictor predictor,
        SignedProbabilities signedProbabilities,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.predictor = predictor;
        this.signedProbabilities = signedProbabilities;
        this.progressTracker = progressTracker;
    }

    @Override
    public void accept(Batch batch) {
        batch.nodeIds().forEach(sourceId -> {
            graph.forEachRelationship(sourceId, -1, (source, target, predictionTarget) -> {
                var predictedProbability = predictor.predictedProbability(source, target);
                var signedProbability = sign(predictionTarget, source, target) * predictedProbability;
                signedProbabilities.add(signedProbability);
                return true;
            });
        });
        progressTracker.logProgress(batch.size());
    }

    private int sign(double predictionTarget, long sourceId, long targetId) {
        switch ((int)predictionTarget) {
            case POSITIVE_LINK:
                return 1;
            case NEGATIVE_LINK:
                return -1;
            default:
                throw new IllegalArgumentException(
                    formatWithLocale(
                        "Invalid property value %.4f on relationship (%d,%d). Valid values are 0 and 1 which represent target classes for links.",
                        predictionTarget,
                        sourceId,
                        targetId
                    ));
        }
    }
}
