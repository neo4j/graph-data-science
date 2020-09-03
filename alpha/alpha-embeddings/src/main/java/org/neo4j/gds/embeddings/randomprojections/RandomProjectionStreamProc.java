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
package org.neo4j.gds.embeddings.randomprojections;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class RandomProjectionStreamProc extends StreamProc<RandomProjection, RandomProjection, RandomProjectionStreamProc.StreamResult, RandomProjectionStreamConfig> {

    @Procedure(value = "gds.alpha.randomProjection.stream", mode = READ)
    @Description(RandomProjectionCompanion.DESCRIPTION)
    public Stream<RandomProjectionStreamProc.StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<RandomProjection, RandomProjection, RandomProjectionStreamConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return stream(computationResult);
    }

    @Override
    protected NodeProperties getNodeProperties(ComputationResult<RandomProjection, RandomProjection, RandomProjectionStreamConfig> computationResult) {
        return RandomProjectionCompanion.getNodeProperties(computationResult);
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        return new StreamResult(originalNodeId, nodeProperties.floatArrayValue(internalNodeId));
    }

    @Override
    protected RandomProjectionStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return RandomProjectionStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<RandomProjection, RandomProjectionStreamConfig> algorithmFactory() {
        return new RandomProjectionFactory<>();
    }

    public static final class StreamResult {
        public final long nodeId;
        public final List<Number> embedding;

        StreamResult(long nodeId, float[] embedding) {
            this.nodeId = nodeId;
            this.embedding = arrayToList(embedding);
        }

        static List<Number> arrayToList(float[] values) {
            var floats = new ArrayList<Number>(values.length);
            for (float value : values) {
                floats.add(value);
            }
            return floats;
        }
    }
}
