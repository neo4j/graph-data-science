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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.GdsVersionInfoProvider;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.graphsage.TrainConfigTransformer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrain;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.MultiLabelGraphSageTrain;
import org.neo4j.gds.embeddings.graphsage.algo.SingleLabelGraphSageTrain;
import org.neo4j.gds.termination.TerminationFlag;

class GraphSageTrainAlgorithmFactory {
    GraphSageTrain create(
        Graph graph,
        GraphSageTrainConfig configuration,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var gdsVersion = GdsVersionInfoProvider.GDS_VERSION_INFO.gdsVersion();

        var parameters = TrainConfigTransformer.toParameters(configuration);

        if (configuration.isMultiLabel()) return new MultiLabelGraphSageTrain(
            graph,
            parameters,
            configuration.projectedFeatureDimension().orElseThrow(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag,
            gdsVersion,
            configuration
        );

        return new SingleLabelGraphSageTrain(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag,
            gdsVersion,
            configuration
        );
    }
}
