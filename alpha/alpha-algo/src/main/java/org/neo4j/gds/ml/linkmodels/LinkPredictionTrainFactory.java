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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.AbstractAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;

public class LinkPredictionTrainFactory extends AbstractAlgorithmFactory<LinkPredictionTrain, LinkPredictionTrainConfig> {

    LinkPredictionTrainFactory() {
        super();
    }

    @TestOnly
    LinkPredictionTrainFactory(ProgressLogger.ProgressLoggerFactory loggerFactory) {
        super(loggerFactory);
    }

    @Override
    protected long taskVolume(
        Graph graph, LinkPredictionTrainConfig configuration
    ) {
        // has to be reset after model selection
        return configuration.params().size() * configuration.validationFolds();
    }

    @Override
    protected String taskName() {
        return "LinkPredictionTrain";
    }

    @Override
    protected LinkPredictionTrain build(
        Graph graph, LinkPredictionTrainConfig configuration, AllocationTracker tracker, ProgressLogger progressLogger
    ) {
        return new LinkPredictionTrain(graph, configuration, progressLogger);
    }

    @Override
    public MemoryEstimation memoryEstimation(LinkPredictionTrainConfig configuration) {
        return MemoryEstimations.builder(LinkPredictionTrain.class)
            .add("algorithm", LinkPredictionTrainEstimation.estimate(configuration))
            .build();
    }
}
