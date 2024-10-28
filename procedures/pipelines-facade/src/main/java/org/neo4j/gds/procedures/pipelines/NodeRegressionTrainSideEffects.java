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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainResult;

import java.util.Optional;

/**
 * We store in catalog and optionally on disk
 */
class NodeRegressionTrainSideEffects implements SideEffect<NodeRegressionTrainResult.NodeRegressionTrainPipelineResult, Void> {
    private final ModelPersister modelPersister;
    private final NodeRegressionPipelineTrainConfig configuration;

    NodeRegressionTrainSideEffects(
        ModelPersister modelPersister,
        NodeRegressionPipelineTrainConfig configuration
    ) {
        this.modelPersister = modelPersister;
        this.configuration = configuration;
    }

    @Override
    public Optional<Void> process(
        GraphResources graphResources,
        Optional<NodeRegressionTrainResult.NodeRegressionTrainPipelineResult> result
    ) {
        if (result.isEmpty()) return Optional.empty();

        var nodeRegressionTrainPipelineResult = result.get();
        var model = nodeRegressionTrainPipelineResult.model();

        modelPersister.persistModel(model, configuration.storeModelToDisk());

        return Optional.empty();
    }
}
