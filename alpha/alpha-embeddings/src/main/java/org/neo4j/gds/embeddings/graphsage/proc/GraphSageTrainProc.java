/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrain;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.TrainProc;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.graphalgo.utils.StringJoining;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.graphsage.proc.GraphSageCompanion.GRAPHSAGE_DESCRIPTION;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class GraphSageTrainProc extends TrainProc<GraphSageTrain, ModelData, GraphSageTrainConfig> {

    @Description(GRAPHSAGE_DESCRIPTION)
    @Procedure(name = "gds.alpha.graphSage.train", mode = Mode.READ)
    public Stream<TrainResult> train(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<GraphSageTrain, Model<ModelData, GraphSageTrainConfig>, GraphSageTrainConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        Model<ModelData, GraphSageTrainConfig> result = computationResult.result();

        ModelCatalog.set(result);
        return Stream.of(trainResult(computationResult));
    }

    @Description(ESTIMATE_DESCRIPTION)
    @Procedure(name = "gds.alpha.graphSage.train.estimate", mode = Mode.READ)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected GraphSageTrainConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return GraphSageTrainConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<GraphSageTrain, GraphSageTrainConfig> algorithmFactory() {
        return new GraphSageTrainAlgorithmFactory();
    }

    @Override
    protected void validateConfigsAndGraphStore(
        GraphStore graphStore, GraphCreateConfig graphCreateConfig, GraphSageTrainConfig config
    ) {
        var nodeLabels = graphStore.nodeLabels();
        if (!config.isMultiLabel()) {
            // all properties exist on all labels
            List<String> missingProperties = config.nodePropertyNames()
                .stream()
                .filter(weightProperty -> !graphStore.hasNodeProperty(nodeLabels, weightProperty))
                .collect(Collectors.toList());
            if (!missingProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node properties %s not found in graph with node properties: %s in all node labels: %s",
                    missingProperties,
                    graphStore.nodePropertyKeys(nodeLabels),
                    StringJoining.join(nodeLabels.stream().map(NodeLabel::name))
                ));
            }
        } else {
            // each property exists on at least one label
            var allProperties = graphStore.nodePropertyKeys().values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
            var missingProperties = config.nodePropertyNames().stream().filter(key -> !allProperties.contains(key)).collect(Collectors.toSet());
            if (!missingProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Each property set in `nodePropertyNames` must exist for one label. Missing properties: %s",
                    missingProperties
                ));
            }
        }
    }

    @Override
    protected void validateGraphStore(GraphStore graphStore) {
        if (graphStore.relationshipCount() == 0) {
            throw new IllegalArgumentException("There should be at least one relationship in the graph.");
        }
    }
}
