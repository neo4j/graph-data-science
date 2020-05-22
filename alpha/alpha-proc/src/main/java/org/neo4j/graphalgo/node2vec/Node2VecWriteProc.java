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
package org.neo4j.graphalgo.node2vec;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.node2vec.Node2Vec;
import org.neo4j.graphalgo.impl.node2vec.Node2VecWriteConfig;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.node2vec.Node2VecStreamProc.NODE2VEC_DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

public class Node2VecWriteProc extends WriteProc<Node2Vec, Node2Vec, Node2VecWriteProc.WriteResult, Node2VecWriteConfig> {

    @Procedure(value = "gds.alpha.node2vec.write", mode = WRITE)
    @Description(NODE2VEC_DESCRIPTION)
    public Stream<Node2VecWriteProc.WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    )  {
        ComputationResult<Node2Vec, Node2Vec, Node2VecWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult);
    }

    @Override
    protected Node2VecWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return Node2VecWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Node2Vec, Node2VecWriteConfig> algorithmFactory(Node2VecWriteConfig config) {
        return new AlphaAlgorithmFactory<>() {
            @Override
            public Node2Vec buildAlphaAlgo(
                Graph graph, Node2VecWriteConfig configuration, AllocationTracker tracker, Log log
            ) {
                return new Node2Vec(graph, config);
            }
        };
    }

    @Override
    protected PropertyTranslator<Node2Vec> nodePropertyTranslator(ComputationResult<Node2Vec, Node2Vec, Node2VecWriteConfig> computationResult) {
        return (PropertyTranslator.OfDoubleArray<Node2Vec>) Node2Vec::embeddingForNode;
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(ComputationResult<Node2Vec, Node2Vec, Node2VecWriteConfig> computeResult) {
        return new WriteResult.Builder();
    }

    public static final class WriteResult {

        public final long nodeCount;
        public final long nodePropertiesWritten;
        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final Map<String, Object> configuration;

        WriteResult(
            long nodeCount,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            Map<String, Object> configuration
        ) {
            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.configuration = configuration;
        }

        static class Builder extends AbstractResultBuilder<WriteResult> {

            @Override
            public WriteResult build() {
                return new WriteResult(
                    nodeCount,
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    writeMillis,
                    config.toMap()
                );
            }
        }
    }

}
