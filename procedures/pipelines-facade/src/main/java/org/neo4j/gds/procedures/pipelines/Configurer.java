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

import org.neo4j.gds.api.User;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

class Configurer {
    private final PipelineRepository pipelineRepository;
    private final User user;

    Configurer(PipelineRepository pipelineRepository, User user) {
        this.pipelineRepository = pipelineRepository;
        this.user = user;
    }

    /**
     * Some dull scaffolding
     */
    <CONFIGURATION> Stream<PipelineInfoResult> configureLinkPredictionTrainingPipeline(
        String pipelineNameAsString,
        Supplier<CONFIGURATION> configurationSupplier,
        BiConsumer<LinkPredictionTrainingPipeline, CONFIGURATION> action
    ) {
        return configure(
            pipelineNameAsString,
            pipelineName -> pipelineRepository.getLinkPredictionTrainingPipeline(user, pipelineName),
            configurationSupplier,
            action,
            PipelineInfoResult::create
        );
    }

    /**
     * Some more dull scaffolding
     */
    <CONFIGURATION> Stream<NodePipelineInfoResult> configureNodeClassificationTrainingPipeline(
        String pipelineNameAsString,
        Supplier<CONFIGURATION> configurationSupplier,
        BiConsumer<NodeClassificationTrainingPipeline, CONFIGURATION> action
    ) {
        return configure(
            pipelineNameAsString,
            pipelineName -> pipelineRepository.getNodeClassificationTrainingPipeline(user, pipelineName),
            configurationSupplier,
            action,
            NodePipelineInfoResult::create
        );
    }

    /**
     * Some dull and very generic scaffolding
     */
    private <CONFIGURATION, PIPELINE, RESULT> Stream<RESULT> configure(
        String pipelineNameAsString,
        Function<PipelineName, PIPELINE> pipelineSupplier,
        Supplier<CONFIGURATION> configurationSupplier,
        BiConsumer<PIPELINE, CONFIGURATION> action,
        BiFunction<PipelineName, PIPELINE, RESULT> resultRenderer
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);
        var pipeline = pipelineSupplier.apply(pipelineName);

        var configuration = configurationSupplier.get();

        action.accept(pipeline, configuration);

        var r = resultRenderer.apply(pipelineName, pipeline);

        return Stream.of(r);
    }
}
