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

package org.neo4j.graphalgo.labelpropagation;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;
import org.neo4j.graphalgo.impl.results.MemoryEstimateResult;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LabelPropagationWriteProc extends LabelPropagationProcBase<LabelPropagationWriteConfig> {

    @Procedure(value = "gds.algo.labelPropagation.write", mode = Mode.WRITE)
    @Description("CALL gds.algo.labelPropagation.write(" +
                 "  graphName: STRING," +
                 "  configuration: MAP {" +
                 "     maxIterations: INTEGER, " +
                 "     weightProperty: STRING, " +
                 "     seedProperty: STRING, " +
                 "     concurrency: INTEGER" +
                 "  }" +
                 ")" +
                 "YIELD" +
                 "  writeProperty: STRING," +
                 "  nodePropertiesWritten: INTEGER," +
                 "  relationshipPropertiesWritten: INTEGER," +
                 "  createMillis: INTEGER," +
                 "  computeMillis: INTEGER," +
                 "  writeMillis: INTEGER" +
                 "  maxIterations: INTEGER," +
                 "  seedProperty: STRING," +
                 "  weightProperty: STRING," +
                 "  postProcessingMillis: INTEGER," +
                 "  communityCount: INTEGER," +
                 "  ranIterations: INTEGER," +
                 "  didConverge: BOOLEAN," +
                 "  communityDistribution: MAP")
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationWriteConfig> result = compute(
            graphNameOrConfig,
            configuration
        );
        return write(result, true);
    }

    @Procedure(value = "gds.algo.labelPropagation.stats", mode = READ)
    @Description("CALL gds.algo.labelPropagation.stats(" +
                 "  graphName: STRING," +
                 "  configuration: MAP {" +
                 "     maxIterations: INTEGER, " +
                 "     weightProperty: STRING, " +
                 "     seedProperty: STRING, " +
                 "     concurrency: INTEGER" +
                 "  }" +
                 ")" +
                 "YIELD" +
                 "  writeProperty: STRING," +
                 "  nodePropertiesWritten: INTEGER," +
                 "  relationshipPropertiesWritten: INTEGER," +
                 "  createMillis: INTEGER," +
                 "  computeMillis: INTEGER," +
                 "  writeMillis: INTEGER" +
                 "  maxIterations: INTEGER," +
                 "  seedProperty: STRING," +
                 "  weightProperty: STRING," +
                 "  postProcessingMillis: INTEGER," +
                 "  communityCount: INTEGER," +
                 "  ranIterations: INTEGER," +
                 "  didConverge: BOOLEAN," +
                 "  communityDistribution: MAP")
    public Stream<LabelPropagationWriteProc.WriteResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult, false);
    }

    private Stream<WriteResult> write(
        ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationWriteConfig> computationResult,
        boolean write
    ) {
        log.debug("Writing results");

        LabelPropagationWriteConfig config = computationResult.config();
        Graph graph = computationResult.graph();
        LabelPropagation result = computationResult.result();

        WriteResultBuilder builder = new WriteResultBuilder(
            config,
            graph.nodeCount(),
            callContext,
            computationResult.tracker()
        );
        builder.withCreateMillis(computationResult.createMillis());
        builder.withComputeMillis(computationResult.computeMillis());

        if (!computationResult.isGraphEmpty()) {
            builder
                .didConverge(result.didConverge())
                .ranIterations(result.ranIterations())
                .withCommunityFunction((nodeId) -> result.labels().get(nodeId));
            if (write) {
                writeNodeProperties(builder, computationResult);
                graph.releaseProperties();
            }
        }
        return Stream.of(builder.build());
    }

    @Procedure(value = "gds.algo.labelPropagation.write.estimate", mode = READ)
    @Description("CALL gds.algo.labelPropagation.write.estimate(" +
                 "  graphName: STRING," +
                 "  configuration: MAP {" +
                 "     maxIterations: INTEGER, " +
                 "     weightProperty: STRING, " +
                 "     seedProperty: STRING, " +
                 "     concurrency: INTEGER" +
                 "  }" +
                 ")" +
                 "YIELD" +
                 "  nodes: INTEGER, " +
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

    @Procedure(value = "gds.algo.labelPropagation.stats.estimate", mode = READ)
    @Description("CALL gds.algo.labelPropagation.stats.estimate(" +
                 "  graphName: STRING," +
                 "  configuration: MAP {" +
                 "     maxIterations: INTEGER, " +
                 "     weightProperty: STRING, " +
                 "     seedProperty: STRING, " +
                 "     concurrency: INTEGER" +
                 "  }" +
                 ") YIELD" +
                 "  nodes: INTEGER, " +
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

    @Override
    protected PropertyTranslator<LabelPropagation> nodePropertyTranslator(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationWriteConfig> computationResult) {

        LabelPropagationWriteConfig config = computationResult.config();

        boolean writePropertyEqualsSeedProperty = config.writeProperty().equals(config.seedProperty());
        NodeProperties seedProperties = computationResult.graph().nodeProperties(config.seedProperty());

        if (writePropertyEqualsSeedProperty) {
            return new PropertyTranslator.OfLongIfChanged<>(
                seedProperties,
                (data, nodeId) -> data.labels().get(nodeId)
            );
        }

        return (PropertyTranslator.OfLong<LabelPropagation>) (data, nodeId) -> data
            .labels()
            .get(nodeId);
    }

    @Override
    public LabelPropagationWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LabelPropagationWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    public static class WriteResultBuilder extends AbstractCommunityResultBuilder<LabelPropagationWriteConfig, WriteResult> {

        private final LabelPropagationWriteConfig config;

        private long ranIterations;
        private boolean didConverge;

        WriteResultBuilder(
            LabelPropagationWriteConfig config,
            long nodeCount,
            ProcedureCallContext context,
            AllocationTracker tracker
        ) {
            super(
                config,
                nodeCount,
                context,
                tracker
            );
            this.config = config;
        }

        LabelPropagationWriteProc.WriteResultBuilder ranIterations(long iterations) {
            this.ranIterations = iterations;
            return this;
        }

        LabelPropagationWriteProc.WriteResultBuilder didConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        @Override
        protected LabelPropagationWriteProc.WriteResult buildResult() {
            return new LabelPropagationWriteProc.WriteResult(
                config,
                nodePropertiesWritten,
                0L,
                createMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                maybeCommunityCount.orElse(-1L),
                ranIterations,
                didConverge,
                communityHistogramOrNull()
            );
        }
    }

    public static class WriteResult {

        public String writeProperty;
        public long nodePropertiesWritten;
        public long relationshipPropertiesWritten;
        public long createMillis;
        public long computeMillis;
        public long writeMillis;
        public long maxIterations;
        public String seedProperty;
        public String weightProperty;
        public long postProcessingMillis;
        public long communityCount;
        public long ranIterations;
        public boolean didConverge;
        public Map<String, Object> communityDistribution;

        WriteResult(
            LabelPropagationWriteConfig config,
            long nodePropertiesWritten,
            long relationshipPropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long communityCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> communityDistribution
        ) {
            this.writeProperty = config.writeProperty();
            this.maxIterations = config.maxIterations();
            this.seedProperty = config.seedProperty();
            this.weightProperty = config.weightProperty();
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.relationshipPropertiesWritten = relationshipPropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.communityCount = communityCount;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.communityDistribution = communityDistribution;
        }
    }
}
