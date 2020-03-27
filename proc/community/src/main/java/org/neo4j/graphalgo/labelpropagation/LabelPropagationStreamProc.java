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
package org.neo4j.graphalgo.labelpropagation;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.labelpropagation.LabelPropagationProc.LABEL_PROPAGATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class LabelPropagationStreamProc extends StreamProc<LabelPropagation, LabelPropagation, LabelPropagationStreamProc.StreamResult, LabelPropagationStreamConfig> {

    @Procedure(value = "gds.labelPropagation.stream", mode = READ)
    @Description(LABEL_PROPAGATION_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.labelPropagation.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected StreamResult streamResult(long nodeId, long originalNodeId, LabelPropagation computationResult) {
        return new StreamResult(originalNodeId, computationResult.labels().get(nodeId));
    }

    @Override
    public LabelPropagationStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LabelPropagationStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<LabelPropagation, LabelPropagationStreamConfig> algorithmFactory(
        LabelPropagationStreamConfig config
    ) {
        return new LabelPropagationFactory<>(config);
    }

    public static final class StreamResult {
        public final long nodeId;
        public final long communityId;

        StreamResult(long nodeId, long communityId) {
            this.nodeId = nodeId;
            this.communityId = communityId;
        }
    }

}
