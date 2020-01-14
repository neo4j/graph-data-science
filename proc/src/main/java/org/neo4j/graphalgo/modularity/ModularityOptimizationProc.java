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
package org.neo4j.graphalgo.modularity;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.LegacyAlgoBaseProc;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.modularity.ModularityOptimization;
import org.neo4j.graphalgo.impl.modularity.ModularityOptimizationFactory;
import org.neo4j.graphalgo.impl.results.MemoryEstimateResult;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.ProcedureConstants.SEED_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.WRITE_PROPERTY_KEY;

public class ModularityOptimizationProc extends LegacyAlgoBaseProc<ModularityOptimization, ModularityOptimization> {

    @Procedure(name = "algo.beta.modularityOptimization.write", mode = Mode.WRITE)
    @Description("CALL algo.beta.modularityOptimization.write(" +
                 "label:String, relationship:String, " +
                 "{iterations: 10, tolerance: 0.0001, direction: 'OUTGOING', write: true, writeProperty: null, concurrency: 4})" +
                 "YIELD modularity, communityCount, ranIterations, didConverge, loadMillis, computeMillis, writeMillis, write, writeProperty, nodes")
    public Stream<WriteResult> betaModularityOptimization(
        @Name(value = "label", defaultValue = "") String label,
        @Name(value = "relationship", defaultValue = "") String relationshipType,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        return run(label, relationshipType, config);
    }

    @Procedure(name = "algo.beta.modularityOptimization.stream", mode = Mode.WRITE)
    @Description("CALL algo.beta.modularityOptimization.stream(" +
                 "label:String, relationship:String, " +
                 "{iterations: 10, tolerance: 0.0001, direction: 'OUTGOING', write: true, writeProperty: null, concurrency: 4})" +
                 "YIELD nodeId, community")
    public Stream<StreamResult> betaModularityOptimizationStream(
        @Name(value = "label", defaultValue = "") String label,
        @Name(value = "relationship", defaultValue = "") String relationshipType,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        return stream(label, relationshipType, config, callContext);
    }

    @Procedure(value = "algo.beta.modularityOptimization.memrec")
    @Description("CALL algo.beta.modularityOptimization.memrec(label:String, relationship:String, {...properties}) " +
                 "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for Modularity Optimization")
    public Stream<MemoryEstimateResult> modularityOptimizationMemrec(
        @Name(value = "label", defaultValue = "") String label,
        @Name(value = "relationship", defaultValue = "") String relationshipType,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {

        ProcedureConfiguration configuration = newConfig(label, relationshipType, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemoryEstimateResult(memoryEstimation));
    }

    public Stream<WriteResult> run(String label, String relationshipType, Map<String, Object> config) {
        ProcedureSetup setup = setup(label, relationshipType, config, callContext);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.of(new WriteResult(
                0, 0, 0, 0, 0,
                setup.procedureConfig.isWriteFlag(true),
                setup.procedureConfig.writeProperty(),
                setup.procedureConfig.writeProperty(),
                false, 0, 0, 0, null
            ));
        }

        ModularityOptimization modularity = compute(setup);

        setup.builder.withCommunityFunction(modularity::getCommunityId);

        Optional<String> writeProperty = setup.procedureConfig.getString(WRITE_PROPERTY_KEY);

        setup.builder
            .withModularity(modularity.getModularity())
            .withRanIterations(modularity.getIterations())
            .withDidConverge(modularity.didConverge());

        if (setup.procedureConfig.isWriteFlag() && writeProperty.isPresent() && !writeProperty.get().equals("")) {
            setup.builder.withCommunityProperty(writeProperty.get());
            write(
                setup.builder,
                setup.graph,
                modularity,
                setup.procedureConfig,
                writeProperty.get()
            );

            setup.graph.releaseProperties();
        }

        return Stream.of(setup.builder.build());
    }

    public Stream<StreamResult> stream(
        String label,
        String relationship,
        Map<String, Object> config,
        ProcedureCallContext context
    ) {

        ProcedureSetup setup = setup(label, relationship, config, context);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.empty();
        }

        ModularityOptimization modularityOptimization = compute(setup);

        return LongStream.range(0, setup.graph.nodeCount())
            .mapToObj(nodeId -> {
                long neoNodeId = setup.graph.toOriginalNodeId(nodeId);
                return new StreamResult(neoNodeId, modularityOptimization.getCommunityId(nodeId));
            });
    }

    @Override
    protected GraphLoader configureGraphLoader(
        GraphLoader loader, ProcedureConfiguration config
    ) {
        final String seedProperty = config.getString(SEED_PROPERTY_KEY, null);
        if (seedProperty != null) {
            loader.withOptionalNodeProperties(PropertyMapping.of(seedProperty, -1));
        }

        return loader.withDirection(config.getDirection(Direction.OUTGOING));
    }

    @Override
    protected AlgorithmFactory<ModularityOptimization, ProcedureConfiguration> algorithmFactory(ProcedureConfiguration config) {
        return new ModularityOptimizationFactory();
    }

    private ModularityOptimization compute(ProcedureSetup setup) {
        final ModularityOptimization modularityOptimization = newAlgorithm(
            setup.graph,
            setup.procedureConfig,
            setup.tracker
        );
        ModularityOptimization algoResult = runWithExceptionLogging(
            ModularityOptimization.class.getSimpleName() + "failed",
            () -> {
                try (ProgressTimer ignore = ProgressTimer.start(setup.builder::withComputeMillis)) {
                    return modularityOptimization.compute();
                }
            }
        );

        log.info(
            ModularityOptimization.class.getSimpleName() + ": overall memory usage %s",
            setup.tracker.getUsageString()
        );

        modularityOptimization.release();
        setup.graph.releaseTopology();

        return algoResult;
    }

    private void write(
        WriteResultBuilder resultBuilder,
        Graph graph,
        ModularityOptimization modularityOptimization,
        ProcedureConfiguration configuration,
        String writeProperty
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
            write(graph, modularityOptimization, configuration, writeProperty);
        }
    }

    private void write(
        Graph graph,
        ModularityOptimization modularityOptimization,
        ProcedureConfiguration procedureConfiguration,
        String writeProperty
    ) {
        log.debug("Writing results");

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, terminationFlag)
            .withLog(log)
            .parallel(
                Pools.DEFAULT,
                procedureConfiguration.getWriteConcurrency()
            )
            .build();
        exporter.write(
            writeProperty,
            modularityOptimization,
            ModularityOptimizationTranslator.INSTANCE
        );
    }

    private ProcedureSetup setup(
        String label,
        String relationship,
        Map<String, Object> config,
        ProcedureCallContext context
    ) {
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        WriteResultBuilder builder = new WriteResultBuilder(configuration, 0, context, tracker);

        Graph graph = loadGraph(configuration, tracker, builder);

        builder.withNodeCount(graph.nodeCount());

        return new ProcedureSetup(builder, graph, tracker, configuration);
    }

    static final class ModularityOptimizationTranslator implements PropertyTranslator.OfLong<ModularityOptimization> {
        public static final ModularityOptimizationTranslator INSTANCE = new ModularityOptimizationTranslator();

        @Override
        public long toLong(ModularityOptimization data, long nodeId) {
            return data.getCommunityId(nodeId);
        }
    }

    public static class ProcedureSetup {
        final WriteResultBuilder builder;
        final Graph graph;
        final AllocationTracker tracker;
        final ProcedureConfiguration procedureConfig;

        ProcedureSetup(
            final WriteResultBuilder builder,
            final Graph graph,
            final AllocationTracker tracker,
            final ProcedureConfiguration procedureConfig
        ) {
            this.builder = builder;
            this.graph = graph;
            this.tracker = tracker;
            this.procedureConfig = procedureConfig;
        }
    }

    public static class WriteResult {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long nodes;
        public final boolean write;
        public final String communityProperty;
        public final String writeProperty;
        public boolean didConverge;
        public long ranIterations;
        public double modularity;
        public final long communityCount;
        public final Map<String, Object> communityDistribution;


        WriteResult(
            long loadMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            long nodes,
            boolean write,
            String communityProperty,
            String writeProperty,
            boolean didConverge,
            long ranIterations,
            double modularity,
            long communityCount,
            Map<String, Object> communityDistribution
        ) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodes = nodes;
            this.write = write;
            this.communityProperty = communityProperty;
            this.writeProperty = writeProperty;
            this.didConverge = didConverge;
            this.ranIterations = ranIterations;
            this.modularity = modularity;
            this.communityCount = communityCount;
            this.communityDistribution = communityDistribution;
        }
    }

    public static class WriteResultBuilder extends AbstractCommunityResultBuilder<ProcedureConfiguration, WriteResult> {
        private String communityProperty;
        private long ranIterations;
        private boolean didConverge;
        private double modularity;

        WriteResultBuilder(ProcedureConfiguration config, long nodeCount, ProcedureCallContext context, AllocationTracker tracker) {
            super(config, nodeCount, context, tracker);
        }

        public WriteResultBuilder withRanIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        public WriteResultBuilder withDidConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        public WriteResultBuilder withCommunityProperty(String communityProperty) {
            this.communityProperty = communityProperty;
            return this;
        }

        public WriteResultBuilder withModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                createMillis,
                computeMillis,
                postProcessingDuration,
                writeMillis,
                nodePropertiesWritten,
                config.isWriteFlag(true),
                communityProperty,
                writeProperty,
                didConverge,
                ranIterations,
                modularity,
                maybeCommunityCount.orElse(0),
                communityHistogramOrNull()
            );
        }

        void withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
        }
    }

    public static class StreamResult {
        public final long nodeId;
        public final long community;

        public StreamResult(long nodeId, long community) {
            this.nodeId = nodeId;
            this.community = community;
        }
    }
}
