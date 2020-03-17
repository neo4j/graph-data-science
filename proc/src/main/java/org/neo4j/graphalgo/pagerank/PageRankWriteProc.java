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

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.pagerank.PageRankProc.PAGE_RANK_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class PageRankWriteProc extends WriteProc<PageRank, PageRank, PageRankWriteProc.WriteResult, PageRankWriteConfig> {

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
        return write(computationResult);
    }

    @Procedure(value = "gds.pageRank.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected PropertyTranslator<PageRank> nodePropertyTranslator(ComputationResult<PageRank, PageRank, PageRankWriteConfig> computationResult) {
        return PageRankProc.ScoresTranslator.INSTANCE;
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(ComputationResult<PageRank, PageRank, PageRankWriteConfig> computeResult) {
        return new WriteResult.Builder()
            .withDidConverge(computeResult.isGraphEmpty() ? false : computeResult.result().didConverge())
            .withRanIterations(computeResult.isGraphEmpty() ? 0 : computeResult.result().iterations());
    }

    @Override
    protected AlgorithmFactory<PageRank, PageRankWriteConfig> algorithmFactory(PageRankWriteConfig config) {
        return PageRankProc.algorithmFactory(config);
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

        static class Builder extends AbstractResultBuilder<WriteResult> {

            private long ranIterations;
            private boolean didConverge;

            Builder withRanIterations(long ranIterations) {
                this.ranIterations = ranIterations;
                return this;
            }

            Builder withDidConverge(boolean didConverge) {
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

}
