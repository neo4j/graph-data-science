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
package org.neo4j.gds.ml.linkmodels.logisticregression;

import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.batch.BatchQueue;
import org.neo4j.gds.ml.batch.HugeBatchQueue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.function.Supplier;

public class LinkLogisticRegressionTrain {

    private final Graph graph;
    private final HugeLongArray trainSet;
    private final LinkLogisticRegressionTrainConfig config;
    private final ProgressLogger progressLogger;

    public LinkLogisticRegressionTrain(
        Graph graph,
        HugeLongArray trainSet,
        LinkLogisticRegressionTrainConfig config,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.trainSet = trainSet;
        this.config = config;
        this.progressLogger = progressLogger;
    }

    public LinkLogisticRegressionPredictor compute() {
        var llrData = LinkLogisticRegressionData.from(
            graph,
            config.featureProperties(),
            LinkFeatureCombiners.valueOf(config.linkFeatureCombiner())
        );
        var objective = new LinkLogisticRegressionObjective(
            llrData,
            config.featureProperties(),
            config.penalty(),
            graph
        );
        // we use a less fine grained progress logging for LP than for NC
        var training = new Training(config, ProgressLogger.NULL_LOGGER, graph.nodeCount());
        Supplier<BatchQueue> queueSupplier = () -> new HugeBatchQueue(trainSet, config.batchSize());
        training.train(objective, queueSupplier, config.concurrency());
        return new LinkLogisticRegressionPredictor(objective.modelData, config.featureProperties());
    }
}
