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
package org.neo4j.gds.beta.node2vec;

import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecAlgorithmFactory;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;
import org.neo4j.gds.ml.core.tensor.FloatVector;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class Node2VecStreamProc extends StreamProc<Node2Vec, HugeObjectArray<FloatVector>, Node2VecStreamProc.StreamResult, Node2VecStreamConfig> {

    @Procedure(value = "gds.beta.node2vec.stream", mode = READ)
    @Description(Node2VecCompanion.DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Node2Vec, HugeObjectArray<FloatVector>, Node2VecStreamConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

       return stream(computationResult);
    }

    @Procedure(value = "gds.beta.node2vec.stream.estimate", mode = READ)
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected Node2VecStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return Node2VecStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AbstractAlgorithmFactory<Node2Vec, Node2VecStreamConfig> algorithmFactory() {
        return new Node2VecAlgorithmFactory<>();
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<Node2Vec, HugeObjectArray<FloatVector>, Node2VecStreamConfig> computationResult) {
        return Node2VecCompanion.nodeProperties(computationResult);
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        return new StreamResult(originalNodeId, nodeProperties.floatArrayValue(internalNodeId));
    }

    @SuppressWarnings("unused")
    public static class StreamResult {
        public long nodeId;
        public List<Double> embedding;

        StreamResult(long nodeId, float[] embedding) {
            this.nodeId = nodeId;
            this.embedding = new ArrayList<>(embedding.length);
            for (var f : embedding) {
                this.embedding.add((double) f);
            }
        }
    }
}
