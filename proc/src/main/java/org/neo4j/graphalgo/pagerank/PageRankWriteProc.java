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
package org.neo4j.graphalgo.pagerank;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class PageRankWriteProc extends PageRankBaseProc<PageRankWriteConfig> {

    @Procedure(value = "gds.pageRank.write", mode = WRITE)
    @Description(PAGE_RANK_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRank, PageRank, PageRankWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult, true);
    }

    @Procedure(value = "gds.pageRank.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<WriteResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRank, PageRank, PageRankWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult, false);
    }

    @Procedure(value = "gds.pageRank.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Procedure(value = "gds.pageRank.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    private Stream<WriteResult> write(
        ComputationResult<PageRank, PageRank, PageRankWriteConfig> computeResult,
        boolean write
    ) {
        if (computeResult.isGraphEmpty()) {
          return Stream.of(
              new WriteResult(
                  computeResult.config().writeProperty(),
                  computeResult.config().relationshipWeightProperty(),
                  computeResult.config().maxIterations(),
                  0,
                  computeResult.createMillis(),
                  0,
                  0,
                  0,
                  computeResult.config().dampingFactor(),
                  false
              )
          );
        } else {
            PageRankWriteConfig config = computeResult.config();
            Graph graph = computeResult.graph();
            PageRank pageRank = computeResult.algorithm();

            WriteResultBuilder builder = new WriteResultBuilder(config);

            builder.withCreateMillis(computeResult.createMillis());
            builder.withComputeMillis(computeResult.computeMillis());
            builder.withRanIterations(pageRank.iterations());
            builder.withDidConverge(pageRank.didConverge());

            if (write && !config.writeProperty().isEmpty()) {
                writeNodeProperties(builder, computeResult);
                graph.releaseProperties();
            }

            return Stream.of(builder.build());
        }
    }

    @Override
    protected PropertyTranslator<PageRank> nodePropertyTranslator(ComputationResult<PageRank, PageRank, PageRankWriteConfig> computationResult) {
        return ScoresTranslator.INSTANCE;
    }

    @Override
    protected PageRankWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return PageRankWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    public static final class WriteResult {

        public String writeProperty;
        public @Nullable String weightProperty;
        public long nodePropertiesWritten;
        public long relationshipPropertiesWritten;
        public long createMillis;
        public long computeMillis;
        public long writeMillis;
        public long maxIterations;
        public long ranIterations;
        public double dampingFactor;
        public boolean didConverge;

        WriteResult(
            String writeProperty,
            @Nullable String weightProperty,
            long maxIterations,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long ranIterations,
            double dampingFactor,
            boolean didConverge
        ) {
            this.relationshipPropertiesWritten = 0;
            this.writeProperty = writeProperty;
            this.weightProperty = weightProperty;
            this.maxIterations = maxIterations;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.dampingFactor = dampingFactor;
        }
    }

    public static class WriteResultBuilder extends AbstractResultBuilder<PageRankWriteConfig, WriteResult> {

        private long ranIterations;
        private boolean didConverge;

        WriteResultBuilder(PageRankWriteConfig config) {
            super(config);
        }

        WriteResultBuilder withRanIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        WriteResultBuilder withDidConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        @Override
        public WriteResult build() {
            return new WriteResult(
                writeProperty,
                config.relationshipWeightProperty(),
                config.maxIterations(),
                nodePropertiesWritten,
                createMillis,
                computeMillis,
                writeMillis,
                ranIterations,
                config.dampingFactor(),
                didConverge
            );
        }
    }

    static final class ScoresTranslator implements PropertyTranslator.OfDouble<PageRank> {
        public static final ScoresTranslator INSTANCE = new ScoresTranslator();

        @Override
        public double toDouble(PageRank pageRank, long nodeId) {
            return pageRank.result().array().get(nodeId);
        }
    }
}
