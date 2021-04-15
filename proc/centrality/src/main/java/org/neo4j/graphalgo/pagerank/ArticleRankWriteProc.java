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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
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

public class ArticleRankWriteProc extends WriteProc<PageRankPregelAlgorithm, PageRankPregelResult, PageRankWriteProc.WriteResult, PageRankPregelWriteConfig> {

    @Procedure(value = "gds.articleRank.write", mode = WRITE)
    @Description(PAGE_RANK_DESCRIPTION)
    public Stream<PageRankWriteProc.WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRankPregelAlgorithm, PageRankPregelResult, PageRankPregelWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult);
    }

    @Procedure(value = "gds.articleRank.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<PageRankPregelAlgorithm, PageRankPregelResult, PageRankPregelWriteConfig> computationResult) {
        return PageRankProc.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<PageRankWriteProc.WriteResult> resultBuilder(ComputationResult<PageRankPregelAlgorithm, PageRankPregelResult, PageRankPregelWriteConfig> computeResult) {
        return PageRankProc.resultBuilder(
            new PageRankWriteProc.WriteResult.Builder(callContext, computeResult.config().concurrency()),
            computeResult
        );
    }

    @Override
    protected AlgorithmFactory<PageRankPregelAlgorithm, PageRankPregelWriteConfig> algorithmFactory() {
        return new PageRankPregelAlgorithmFactory<>(PageRankPregelAlgorithmFactory.Mode.ARTICLE_RANK);
    }

    @Override
    protected void validateConfigs(GraphCreateConfig graphCreateConfig, PageRankPregelWriteConfig config) {
        super.validateConfigs(graphCreateConfig, config);
        PageRankProc.validateAlgoConfig(config, log);
    }

    @Override
    protected PageRankPregelWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return PageRankPregelWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }
}
