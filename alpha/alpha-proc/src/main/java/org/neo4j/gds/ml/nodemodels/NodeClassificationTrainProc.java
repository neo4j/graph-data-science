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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.TrainProc;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.GraphStoreValidation;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreWithConfig;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.utils.StringJoining;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class NodeClassificationTrainProc extends TrainProc<NodeClassificationTrain, NodeLogisticRegressionData, NodeClassificationTrainConfig> {

    @Procedure(name = "gds.alpha.ml.nodeClassification.train", mode = Mode.READ)
    @Description("Trains a node classification model")
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
    protected void validateConfigsAndGraphStore(
        GraphStoreWithConfig graphStoreWithConfig, NodeClassificationTrainConfig config
    ) {
        GraphStore graphStore = graphStoreWithConfig.graphStore();
        Collection<NodeLabel> filterLabels = config.nodeLabelIdentifiers(graphStore);
        if (!graphStore.hasNodeProperty(filterLabels, config.targetProperty())) {
            throw new IllegalArgumentException(formatWithLocale(
                "`%s`: `%s` not found in graph with node properties: %s",
                "targetProperty",
                config.targetProperty(),
                StringJoining.join(graphStore.nodePropertyKeys(filterLabels))
            ));
        }
        GraphStoreValidation.validate(
            graphStoreWithConfig,
            config
        );
    }

    @Override
    protected NodeClassificationTrainConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return NodeClassificationTrainConfig.of(
            graphName,
            maybeImplicitCreate,
            username,
            config
        );
    }

    @Override
    protected AlgorithmFactory<NodeClassificationTrain, NodeClassificationTrainConfig> algorithmFactory() {
        return new NodeClassificationTrainAlgorithmFactory();
    }

}
