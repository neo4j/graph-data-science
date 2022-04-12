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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.kmeans.KmeansProc.Kmeans_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.kmeans.stream", description = Kmeans_DESCRIPTION, executionMode = ExecutionMode.STREAM)
public class KmeansStreamProc extends StreamProc<
    Kmeans,
    KmeansResult,
    KmeansStreamProc.StreamResult,
    KmeansStreamConfig> {

    @Procedure(value = "gds.alpha.kmeans.stream", mode = READ)
    @Description(Kmeans_DESCRIPTION)
    public Stream<KmeansStreamProc.StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Kmeans, KmeansResult, KmeansStreamConfig> computationResult = compute(
            graphName,
            configuration
        );
        return stream(computationResult);
    }

    @Override
    protected KmeansStreamConfig newConfig(String username, CypherMapWrapper config) {
        return KmeansStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Kmeans, KmeansStreamConfig> algorithmFactory() {
        return KmeansProc.algorithmFactory();
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<Kmeans, KmeansResult, KmeansStreamConfig> computationResult) {
        return KmeansProc.nodeProperties(computationResult);
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        return new StreamResult(originalNodeId, nodeProperties.longValue(internalNodeId));
    }


    @SuppressWarnings("unused")
    public static class StreamResult {

        public final long nodeId;

        public final long communityId;

        public StreamResult(long nodeId, long communityId) {
            this.nodeId = nodeId;
            this.communityId = communityId;
        }
    }
}
