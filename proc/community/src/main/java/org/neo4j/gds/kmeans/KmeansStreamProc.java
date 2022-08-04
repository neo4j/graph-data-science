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

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class KmeansStreamProc extends AlgoBaseProc<
    Kmeans,
    KmeansResult,
    KmeansStreamConfig,
    KmeansStreamProc.StreamResult
    > {

    static final String KMEANS_DESCRIPTION =
        "The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance";

    @Procedure(value = "gds.alpha.kmeans.stream", mode = READ)
    @Description(KMEANS_DESCRIPTION)
    public Stream<KmeansStreamProc.StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        var streamSpec = new KmeansStreamSpec();
        return new ProcedureExecutor<>(
            streamSpec,
            executionContext()
        ).compute(graphName, configuration, true, true);
    }

    @Procedure(value = "gds.alpha.kmeans.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        var writeSpec = new KmeansStreamSpec();

        return new MemoryEstimationExecutor<>(
            writeSpec,
            executionContext()
        ).computeEstimate(graphName, configuration);
    }

    @Override
    public AlgorithmFactory<?, Kmeans, KmeansStreamConfig> algorithmFactory() {
        return new KmeansStreamSpec().algorithmFactory();
    }

    @Override
    public ComputationResultConsumer<Kmeans, KmeansResult, KmeansStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return new KmeansStreamSpec().computationResultConsumer();
    }

    @Override
    protected KmeansStreamConfig newConfig(String username, CypherMapWrapper config) {
        return new KmeansStreamSpec().newConfigFunction().apply(username, config);
    }


    @SuppressWarnings("unused")
    public static class StreamResult {

        public final long nodeId;

        public final long communityId;
        public final double distanceFromCentroid;

        public final double silhouette;


        public StreamResult(long nodeId, long communityId, double distanceFromCentroid, double silhouette) {
            this.nodeId = nodeId;
            this.communityId = communityId;
            this.distanceFromCentroid = distanceFromCentroid;
            this.silhouette = silhouette;
        }
    }
}
