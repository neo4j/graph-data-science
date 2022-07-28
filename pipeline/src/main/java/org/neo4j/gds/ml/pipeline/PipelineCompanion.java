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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.utils.StringJoining;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.metrics.classification.OutOfBagError.OUT_OF_BAG_ERROR;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class PipelineCompanion {

    public static final String ANONYMOUS_GRAPH = "__ANONYMOUS_GRAPH__";

    private PipelineCompanion() {}

    public static void preparePipelineConfig(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        // TODO: this will go away once node property steps do not modify the graph store with the given graphName
        //  In the future it might operate on a shallow copy instead.
        if (graphNameOrConfiguration instanceof String) {
            algoConfiguration.put("graphName", graphNameOrConfiguration);
        } else {
            algoConfiguration.put("graphName", ANONYMOUS_GRAPH);
        }
    }

    public static <PIPELINE extends TrainingPipeline<?>, INFO_RESULT> Stream<INFO_RESULT> configureAutoTuning(
        String userName,
        String pipelineName,
        Map<String, Object> configMap,
        Function<PIPELINE, INFO_RESULT> factory
    ) {
        PIPELINE pipeline = (PIPELINE) PipelineCatalog.get(userName, pipelineName);

        var cypherConfig = CypherMapWrapper.create(configMap);
        var config = AutoTuningConfig.of(cypherConfig);

        cypherConfig.requireOnlyKeysFrom(config.configKeys());

        pipeline.setAutoTuningConfig(config);

        return Stream.of(factory.apply(pipeline));
    }

    public static void validateMainMetric(TrainingPipeline<?> pipeline, String mainMetric) {
        if (mainMetric.equals(OUT_OF_BAG_ERROR.name())) {
            var nonRFMethods = pipeline.trainingParameterSpace().entrySet().stream()
                .filter(entry -> entry.getKey() != TrainingMethod.RandomForestClassification && !entry.getValue().isEmpty() )
                .map(Map.Entry::getKey)
                .map(Enum::toString)
                .collect(Collectors.toSet());
            if (!nonRFMethods.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "If %s is used as the main metric (the first one), then only RandomForest model candidates are allowed." +
                    " Incompatible training methods used are: %s.",
                    OUT_OF_BAG_ERROR.name(),
                    StringJoining.join(nonRFMethods)
                ));
            }
        }
    }

}
