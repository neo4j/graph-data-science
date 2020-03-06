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

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.result.AbstractResultBuilder;

import java.util.Map;
import java.util.stream.Stream;

abstract class PageRankBaseProc<CONFIG extends PageRankBaseConfig> extends AlgoBaseProc<PageRank, PageRank, CONFIG> {

    static final String PAGE_RANK_DESCRIPTION =
        "Page Rank is an algorithm that measures the transitive influence or connectivity of nodes.";

    @Override
    protected final PageRankFactory<CONFIG> algorithmFactory(PageRankBaseConfig config) {
        if (config.relationshipWeightProperty() == null) {
            return new PageRankFactory<>();
        }
        return new PageRankFactory<>(PageRankAlgorithmType.WEIGHTED);
    }

    protected Stream<WriteResult> write(ComputationResult<PageRank, PageRank, CONFIG> computeResult) {
        return writeOrMutate(computeResult,
            (writeBuilder, computationResult) -> writeNodeProperties(writeBuilder, computationResult)
        );
    }

    protected Stream<WriteResult> mutate(ComputationResult<PageRank, PageRank, CONFIG> computeResult) {
        return writeOrMutate(computeResult,
            (writeBuilder, computationResult) -> mutateNodeProperties(writeBuilder, computationResult)
        );
    }

    protected Stream<WriteResult> writeOrMutate(
        ComputationResult<PageRank, PageRank, CONFIG> computeResult,
        WriteOrMutate<PageRank, PageRank, CONFIG> op
    ) {
        CONFIG config = computeResult.config();
        PageRankWriteConfig writeConfig = ImmutablePageRankWriteConfig.builder()
            .writeProperty("stats does not support a write property")
            .from(config)
            .build();
        if (computeResult.isGraphEmpty()) {
            return Stream.of(
                new PageRankWriteProc.WriteResult(
                    0,
                    computeResult.createMillis(),
                    0,
                    0,
                    0,
                    false,
                    config.toMap()
                )
            );
        } else {
            Graph graph = computeResult.graph();
            PageRank pageRank = computeResult.algorithm();

            WriteResultBuilder builder = new WriteResultBuilder();

            builder.withCreateMillis(computeResult.createMillis());
            builder.withComputeMillis(computeResult.computeMillis());
            builder.withRanIterations(pageRank.iterations());
            builder.withDidConverge(pageRank.didConverge());
            builder.withConfig(config);

            if (shouldWrite(config) && !writeConfig.writeProperty().isEmpty()) {
                op.apply(builder, computeResult);
                graph.releaseProperties();
            }

            return Stream.of(builder.build());
        }
    }

    public static final class WriteResult {

        public long nodePropertiesWritten;
        public long createMillis;
        public long computeMillis;
        public long writeMillis;
        public long ranIterations;
        public boolean didConverge;
        public Map<String, Object> configuration;

        WriteResult(
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> configuration
        ) {
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.configuration = configuration;
        }
    }

    public static final class StatsResult {

        public long createMillis;
        public long computeMillis;
        public long ranIterations;
        public boolean didConverge;
        public Map<String, Object> configuration;

        StatsResult(
            long createMillis,
            long computeMillis,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.configuration = configuration;
        }

        public static StatsResult from(WriteResult writeResult) {
            return new StatsResult(
                writeResult.createMillis,
                writeResult.computeMillis,
                writeResult.ranIterations,
                writeResult.didConverge,
                writeResult.configuration
            );
        }
    }

    static class WriteResultBuilder extends AbstractResultBuilder<WriteResult> {

        private long ranIterations;
        private boolean didConverge;

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
                nodePropertiesWritten,
                createMillis,
                computeMillis,
                writeMillis,
                ranIterations,
                didConverge,
                config.toMap()
            );
        }
    }
}
