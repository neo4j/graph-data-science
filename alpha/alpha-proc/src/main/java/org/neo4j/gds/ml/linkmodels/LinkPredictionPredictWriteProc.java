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

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteRelationshipsProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.StandardWriteRelationshipsResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.linkmodels.LinkPredictionPredictCompanion.DESCRIPTION;
import static org.neo4j.graphalgo.config.GraphCreateConfigValidations.validateIsUndirectedGraph;

public class LinkPredictionPredictWriteProc extends WriteRelationshipsProc<LinkPredictionPredict, LinkPredictionResult, StandardWriteRelationshipsResult, LinkPredictionPredictWriteConfig> {

    @Procedure(name = "gds.alpha.ml.linkPrediction.predict.write", mode = Mode.READ)
    @Description(DESCRIPTION)
    public Stream<StandardWriteRelationshipsResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphNameOrConfig, configuration));
    }

    @Override
    protected void validateConfigsBeforeLoad(GraphCreateConfig graphCreateConfig, LinkPredictionPredictWriteConfig config) {
        validateIsUndirectedGraph(graphCreateConfig, config);
    }

    @Override
    protected LinkPredictionPredictWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LinkPredictionPredictWriteConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    @Override
    protected AlgorithmFactory<LinkPredictionPredict, LinkPredictionPredictWriteConfig> algorithmFactory() {
        return new LinkPredictionPredictFactory<>();
    }

    @Override
    protected AbstractResultBuilder<StandardWriteRelationshipsResult> resultBuilder(
        ComputationResult<LinkPredictionPredict, LinkPredictionResult, LinkPredictionPredictWriteConfig> computeResult
    ) {
        return new StandardWriteRelationshipsResult.Builder();
    }
}
