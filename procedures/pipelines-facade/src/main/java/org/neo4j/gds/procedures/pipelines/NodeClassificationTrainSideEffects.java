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
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationModelResult;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;

import java.util.Optional;

/**
 * We store in catalog and optionally on disk
 */
class NodeClassificationTrainSideEffects implements SideEffect<NodeClassificationModelResult, Void> {
    private final Log log;
    private final ModelCatalog modelCatalog;
    private final ModelRepository modelRepository;
    private final NodeClassificationPipelineTrainConfig configuration;

    NodeClassificationTrainSideEffects(
        Log log,
        ModelCatalog modelCatalog,
        ModelRepository modelRepository,
        NodeClassificationPipelineTrainConfig configuration
    ) {
        this.log = log;
        this.modelCatalog = modelCatalog;
        this.modelRepository = modelRepository;
        this.configuration = configuration;
    }

    @Override
    public Optional<Void> process(
        GraphResources graphResources,
        Optional<NodeClassificationModelResult> result
    ) {
        if (result.isEmpty()) return Optional.empty();

        var nodeClassificationModelResult = result.get();
        var model = nodeClassificationModelResult.model();

        modelCatalog.set(model);

        if (!configuration.storeModelToDisk()) return Optional.empty(); // all done

        try {
            modelRepository.store(model);

            return Optional.empty();
        } catch (Exception e) { // this is terrifying engineering, inherited from ...
            log.error("Failed to store model to disk after training.", e);
            throw e;
        }
    }
}
