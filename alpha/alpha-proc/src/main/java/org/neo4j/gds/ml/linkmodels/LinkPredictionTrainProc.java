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

import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.splitting.EdgeSplitter;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.TrainProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.config.GraphCreateConfigValidations.validateIsUndirectedGraph;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class LinkPredictionTrainProc extends
    TrainProc<LinkPredictionTrain, LinkLogisticRegressionData, LinkPredictionTrainConfig> {

    @Procedure(name = "gds.alpha.ml.linkPrediction.train", mode = Mode.READ)
    @Description("Trains a link prediction model")
    public Stream<MLTrainResult> train(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(
            graphNameOrConfig,
            configuration
        );
        ModelCatalog.set(result.result());
        return Stream.of(new MLTrainResult(result.result(), result.computeMillis()));
    }

    @Override
    protected LinkPredictionTrainConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        var lpConfig = LinkPredictionTrainConfig.of(username, graphName, maybeImplicitCreate, config);
        var trainType = lpConfig.trainRelationshipType();
        var testType = lpConfig.testRelationshipType();
        return ImmutableLinkPredictionTrainConfig
            .builder()
            .from(lpConfig)
            .relationshipTypes(List.of(trainType.name, testType.name))
            .relationshipWeightProperty(EdgeSplitter.RELATIONSHIP_PROPERTY)
            .build();
    }

    @Override
    protected void validateConfigs(
        GraphCreateConfig graphCreateConfig,
        LinkPredictionTrainConfig config
    ) {
        validateIsUndirectedGraph(graphCreateConfig, config);

        if (config.params().isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale("No model candidates (params) specified, we require at least one"));
        }
    }

    @Override
    protected AlgorithmFactory<LinkPredictionTrain, LinkPredictionTrainConfig> algorithmFactory() {
        return new LinkPredictionTrainFactory();
    }
}
