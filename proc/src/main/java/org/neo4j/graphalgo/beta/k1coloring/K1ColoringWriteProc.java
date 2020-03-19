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
package org.neo4j.graphalgo.beta.k1coloring;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.beta.k1coloring.K1ColoringProc.K1_COLORING_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class K1ColoringWriteProc extends WriteProc<K1Coloring, HugeLongArray, K1ColoringWriteProc.WriteResult, K1ColoringWriteConfig> {
    private static final String COLOR_COUNT_FIELD_NAME = "colorCount";

    @Procedure(name = "gds.beta.k1coloring.write", mode = WRITE)
    @Description(K1_COLORING_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<K1Coloring, HugeLongArray, K1ColoringWriteConfig> computationResult =
            compute(graphNameOrConfig, configuration);

        return computationResult.isGraphEmpty() ? Stream.empty() : write(computationResult);
    }

    @Procedure(value = "gds.beta.k1coloring.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected PropertyTranslator<HugeLongArray> nodePropertyTranslator(ComputationResult computationResult) {
        return HugeLongArray.Translator.INSTANCE;
    }


    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(ComputationResult<K1Coloring, HugeLongArray, K1ColoringWriteConfig> computeResult) {
        WriteResult.Builder builder = new WriteResult.Builder(callContext, computeResult.tracker());

        if (callContext.outputFields().anyMatch((field) -> field.equals(COLOR_COUNT_FIELD_NAME))) {
            builder.withColorCount(computeResult.algorithm().usedColors().cardinality());
        }

        return K1ColoringProc.resultBuilder(builder, computeResult);
    }

    @Override
    protected AlgorithmFactory<K1Coloring, K1ColoringWriteConfig> algorithmFactory(K1ColoringWriteConfig config) {
        return new K1ColoringFactory<>();
    }

    @Override
    protected K1ColoringWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return K1ColoringWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    public static class WriteResult {

        public static final WriteResult EMPTY = new WriteResult(
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            null
        );

        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;

        public final long nodes;
        public final long colorCount;
        public final long ranIterations;
        public final boolean didConverge;

        public Map<String, Object> configuration;

        WriteResult(
            long createMillis,
            long computeMillis,
            long writeMillis,
            long nodes,
            long colorCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodes = nodes;
            this.colorCount = colorCount;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.configuration = configuration;
        }

        static class Builder extends K1ColoringProc.K1ColoringResultBuilder<WriteResult> {

            Builder(
                ProcedureCallContext context,
                AllocationTracker tracker
            ) {
                super(context, tracker);
            }

            @Override
            protected WriteResult buildResult() {
                return new WriteResult(
                    createMillis,
                    computeMillis,
                    writeMillis,
                    nodeCount,
                    colorCount,
                    ranIterations,
                    didConverge,
                    config.toMap()
                );
            }
        }
    }

}
