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
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.result.AbstractResultBuilder;

import java.util.stream.Stream;

abstract class PageRankBaseProc<CONFIG extends PageRankBaseConfig> extends AlgoBaseProc<PageRank, PageRank, CONFIG> {

    static final String PAGE_RANK_DESCRIPTION =
        "PageRank is an algorithm that measures the transitive influence or connectivity of nodes.";

    @Override
    protected final PageRankFactory<CONFIG> algorithmFactory(PageRankBaseConfig config) {
        if (config.relationshipWeightProperty() == null) {
            return new PageRankFactory<>();
        }
        return new PageRankFactory<>(PageRankAlgorithmType.WEIGHTED);
    }

    @Override
    protected boolean legacyMode() {
        return false;
    }

    protected Stream<WriteResult> write(ComputationResult<PageRank, PageRank, CONFIG> computeResult) {
        CONFIG config = computeResult.config();
        boolean write = config instanceof PageRankWriteConfig;
        PageRankWriteConfig writeConfig = ImmutablePageRankWriteConfig.builder()
            .writeProperty("stats does not support a write property")
            .from(config)
            .build();
        if (computeResult.isGraphEmpty()) {
            return Stream.of(
                new PageRankWriteProc.WriteResult(
                    writeConfig.writeProperty(),
                    writeConfig.relationshipWeightProperty(),
                    writeConfig.maxIterations(),
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
            Graph graph = computeResult.graph();
            PageRank pageRank = computeResult.algorithm();

            WriteResultBuilder builder = new WriteResultBuilder(writeConfig);

            builder.withCreateMillis(computeResult.createMillis());
            builder.withComputeMillis(computeResult.computeMillis());
            builder.withRanIterations(pageRank.iterations());
            builder.withDidConverge(pageRank.didConverge());

            if (write && !writeConfig.writeProperty().isEmpty()) {
                writeNodeProperties(builder, computeResult);
                graph.releaseProperties();
            }

            return Stream.of(builder.build());
        }
    }

    public static final class WriteResult {

        public String writeProperty;
        public @Nullable String relationshipWeightProperty;
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
            @Nullable String relationshipWeightProperty,
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
            this.relationshipWeightProperty = relationshipWeightProperty;
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

    static class WriteResultBuilder extends AbstractResultBuilder<PageRankWriteConfig, WriteResult> {

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
}
