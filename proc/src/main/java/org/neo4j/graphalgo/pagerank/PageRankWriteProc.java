/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphalgo.impl.results.AbstractResultBuilder;
import org.neo4j.graphalgo.impl.results.MemoryEstimateResult;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class PageRankWriteProc extends PageRankProcBase<PageRankWriteConfig> {

    @Procedure(value = "gds.algo.pageRank.write", mode = WRITE)
    @Description("CALL gds.algo.pageRank.write(graphName: STRING, configuration: MAP {" +
                 "    iterations: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    dampingFactor: FLOAT" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  ranIterations: INTEGER," +
                 "  writeProperty: STRING," +
                 "  nodePropertiesWritten: INTEGER," +
                 "  relationshipPropertiesWritten: INTEGER," +
                 "  createMillis: INTEGER," +
                 "  computeMillis: INTEGER," +
                 "  writeMillis: INTEGER," +
                 "  maxIterations: INTEGER," +
                 "  weightProperty: STRING," +
                 "  postProcessingMillis: INTEGER," +
                 "  didConverge: BOOLEAN")
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

    @Procedure(value = "gds.algo.pageRank.stats", mode = READ)
    @Description("CALL gds.algo.pageRank.stats(graphName: STRING, configuration: MAP {" +
                 "    iterations: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    dampingFactor: FLOAT" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  ranIterations: INTEGER," +
                 "  writeProperty: STRING," +
                 "  nodePropertiesWritten: INTEGER," +
                 "  relationshipPropertiesWritten: INTEGER," +
                 "  createMillis: INTEGER," +
                 "  computeMillis: INTEGER," +
                 "  writeMillis: INTEGER," +
                 "  maxIterations: INTEGER," +
                 "  weightProperty: STRING," +
                 "  postProcessingMillis: INTEGER," +
                 "  didConverge: BOOLEAN")
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

    @Procedure(value = "gds.algo.pageRank.write.estimate", mode = READ)
    @Description("CALL gds.algo.pageRank.write.estimate(graphName: STRING, configuration: MAP {" +
                 "    iterations: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    dampingFactor: FLOAT" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  nodes: INTEGER, "+
                 "  relationships: INTEGER," +
                 "  bytesMin: INTEGER," +
                 "  bytesMax: INTEGER," +
                 "  requiredMemory: STRING," +
                 "  mapView: MAP," +
                 "  treeView: STRING")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeMemoryEstimate(graphNameOrConfig, configuration);
    }

    @Procedure(value = "gds.algo.pageRank.stats.estimate", mode = READ)
    @Description("CALL gds.algo.pageRank.stats.estimate(graphName: STRING, configuration: MAP {" +
                 "    iterations: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    dampingFactor: FLOAT" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  nodes: INTEGER, "+
                 "  relationships: INTEGER," +
                 "  bytesMin: INTEGER," +
                 "  bytesMax: INTEGER," +
                 "  requiredMemory: STRING," +
                 "  mapView: MAP," +
                 "  treeView: STRING")
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeMemoryEstimate(graphNameOrConfig, configuration);
    }

    private Stream<WriteResult> write(
        ComputationResult<PageRank, PageRank, PageRankWriteConfig> computeResult,
        boolean write
    ) {
        if (computeResult.isEmpty()) {
          return Stream.of(
              new WriteResult(
                  computeResult.config(),
                  0,
                  computeResult.createMillis(),
                  0,
                  0,
                  0,
                  false
              )
          );
        } else {
            PageRankWriteConfig config = computeResult.config();
            Graph graph = computeResult.graph();
            PageRank pageRank = computeResult.algorithm();

            WriteResultBuilder builder = new WriteResultBuilder(config);

            builder.setLoadMillis(computeResult.createMillis());
            builder.setComputeMillis(computeResult.computeMillis());
            builder.withNodeCount(graph.nodeCount());
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
    protected Optional<PropertyTranslator<PageRank>> nodePropertyTranslator(ComputationResult<PageRank, PageRank, PageRankWriteConfig> computationResult) {
        return Optional.of(CommunitiesTranslator.INSTANCE);
    }

    @Override
    PageRankWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return PageRankWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    public static final class WriteResult {

        public String writeProperty;
        public String weightProperty;
        public long nodePropertiesWritten;
        public long relationshipPropertiesWritten;
        public long createMillis;
        public long computeMillis;
        public long writeMillis;
        public long maxIterations;
        public long ranIterations;
        public boolean didConverge;

        WriteResult(
            PageRankWriteConfig config,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long ranIterations,
            boolean didConverge
        ) {
            this.relationshipPropertiesWritten = 0;

            this.writeProperty = config.writeProperty();
            this.weightProperty = config.weightProperty();
            this.maxIterations = config.maxIterations();

            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
        }
    }

    public static class WriteResultBuilder extends AbstractResultBuilder<WriteResult> {

        private final PageRankWriteConfig config;

        private long ranIterations;
        private boolean didConverge;

        WriteResultBuilder(PageRankWriteConfig config) {
            this.config = config;
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
                config,
                nodeCount,  // should be nodePropertiesWritten
                loadMillis,
                computeMillis,
                writeMillis,
                ranIterations,
                didConverge
            );
        }
    }

    static final class CommunitiesTranslator implements PropertyTranslator.OfDouble<PageRank> {
        public static final CommunitiesTranslator INSTANCE = new CommunitiesTranslator();

        @Override
        public double toDouble(PageRank pageRank, long nodeId) {
            return pageRank.result().array().get(nodeId);
        }
    }
}
