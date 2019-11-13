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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.multistepscc.MultistepSCC;
import org.neo4j.graphalgo.impl.scc.ForwardBackwardScc;
import org.neo4j.graphalgo.impl.scc.SCCAlgorithm;
import org.neo4j.graphalgo.impl.scc.SCCTarjan;
import org.neo4j.graphalgo.impl.scc.SCCTunedTarjan;
import org.neo4j.graphalgo.results.SCCResult;
import org.neo4j.graphalgo.results.SCCStreamResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class StronglyConnectedComponentsProc extends LabsProc {

    public static final String CONFIG_WRITE_PROPERTY = "writeProperty";
    public static final String CONFIG_OLD_WRITE_PROPERTY = "partitionProperty";
    public static final String CONFIG_CLUSTER = "partition";

    // default algo.scc -> iterative tarjan
    @Procedure(value = "algo.scc", mode = Mode.WRITE)
    @Description("CALL algo.scc(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccDefaultMethod(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return sccIterativeTarjan(label, relationship, config);
    }

    // default algo.scc -> iter tarjan
    @Procedure(value = "algo.scc.stream", mode = READ)
    @Description("CALL algo.scc.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCAlgorithm.StreamResult> sccDefaultMethodStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return sccIterativeTarjanStream(label, relationship, config);
    }

    // algo.scc.tarjan
    @Procedure(value = "algo.scc.recursive.tarjan", mode = Mode.WRITE)
    @Description("CALL algo.scc.tarjan(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccTarjan(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        SCCResult.Builder builder = SCCResult.builder();

        ProgressTimer loadTimer = builder.timeLoad();
        AllocationTracker tracker = AllocationTracker.create();
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());
        loadTimer.stop();

        if (graph.isEmpty()) {
            return Stream.of(SCCResult.EMPTY);
        }

        SCCTarjan tarjan = new SCCTarjan(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(Tarjan)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(tarjan::compute);

        final int[] connectedComponents = tarjan.getConnectedComponents();
        if (configuration.isWriteFlag()) {
            builder.withWrite(true);
            String partitionProperty = configuration.get(CONFIG_WRITE_PROPERTY,
                    CONFIG_OLD_WRITE_PROPERTY,
                    CONFIG_CLUSTER);
            builder.withPartitionProperty(partitionProperty);

            builder.timeWrite(() -> {
                graph.release();
                tarjan.release();
                NodePropertyExporter.of(api, graph, tarjan.terminationFlag)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getWriteConcurrency())
                        .build()
                        .write(
                                partitionProperty,
                                connectedComponents,
                                Translators.OPTIONAL_INT_ARRAY_TRANSLATOR
                        );
            });
        }

        return Stream.of(builder.build(tracker, graph.nodeCount(), l -> (long) connectedComponents[((int) l)]));
    }

    // algo.scc.tunedTarjan
    @Procedure(value = "algo.scc.recursive.tunedTarjan", mode = Mode.WRITE)
    @Description("CALL algo.scc.recursive.tunedTarjan(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccTunedTarjan(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        SCCResult.Builder builder = SCCResult.builder();

        ProgressTimer loadTimer = builder.timeLoad();
        AllocationTracker tracker = AllocationTracker.create();
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());
        loadTimer.stop();

        if (graph.isEmpty()) {
            return Stream.of(SCCResult.EMPTY);
        }

        SCCTunedTarjan tarjan = new SCCTunedTarjan(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(TunedTarjan)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(tarjan::compute);

        if (configuration.isWriteFlag()) {
            builder.withWrite(true);
            String partitionProperty = configuration.get(CONFIG_WRITE_PROPERTY,
                    CONFIG_OLD_WRITE_PROPERTY,
                    CONFIG_CLUSTER);
            builder.withPartitionProperty(partitionProperty);

            builder.timeWrite(() -> NodePropertyExporter
                    .of(api, graph, tarjan.terminationFlag)
                    .withLog(log)
                    .parallel(Pools.DEFAULT, configuration.getWriteConcurrency())
                    .build()
                    .write(
                            partitionProperty,
                            tarjan.getConnectedComponents(),
                            Translators.OPTIONAL_INT_ARRAY_TRANSLATOR
                    ));
        }

        final int[] connectedComponents = tarjan.getConnectedComponents();
        return Stream.of(builder.build(tracker, graph.nodeCount(), l -> (long) connectedComponents[((int) l)]));
    }

    // algo.scc.tunedTarjan.stream
    @Procedure(name = "algo.scc.recursive.tunedTarjan.stream", mode = READ)
    @Description("CALL algo.scc.recursive.tunedTarjan.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "nodeId, partition")
    public Stream<SCCAlgorithm.StreamResult> sccTunedTarjanStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        if (graph.isEmpty()) {
            return Stream.empty();
        }

        return new SCCTunedTarjan(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(TunedTarjan)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute()
                .resultStream();
    }

    // algo.scc.iterative
    @Procedure(value = "algo.scc.iterative", mode = Mode.WRITE)
    @Description("CALL algo.scc.iterative(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccIterativeTarjan(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        final SCCResult.Builder builder = SCCResult.builder();

        final ProgressTimer loadTimer = builder.timeLoad();
        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());
        loadTimer.stop();

        if (graph.isEmpty()) {
            return Stream.of(SCCResult.EMPTY);
        }

        final AllocationTracker tracker = AllocationTracker.create();
        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        final SCCAlgorithm tarjan = SCCAlgorithm.iterativeTarjan(graph, tracker)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(IterativeTarjan)"))
                .withTerminationFlag(terminationFlag);

        builder.timeEval(tarjan::compute);

        if (configuration.isWriteFlag()) {
            builder.withWrite(true);
            String partitionProperty = configuration.get(CONFIG_WRITE_PROPERTY,
                    CONFIG_OLD_WRITE_PROPERTY,
                    CONFIG_CLUSTER);
            builder.withPartitionProperty(partitionProperty).withWriteProperty(partitionProperty);

            builder.timeWrite(() -> NodePropertyExporter.of(api, graph, terminationFlag)
                    .withLog(log)
                    .parallel(Pools.DEFAULT, configuration.getWriteConcurrency())
                    .build()
                    .write(
                            partitionProperty,
                            tarjan.getConnectedComponents(),
                            HugeLongArray.Translator.INSTANCE
                    ));
        }

        final HugeLongArray connectedComponents = tarjan.getConnectedComponents();
        tarjan.release();
        graph.release();
        return Stream.of(builder.build(tracker, graph.nodeCount(), connectedComponents::get));
    }

    // algo.scc.iterative.stream
    @Procedure(name = "algo.scc.iterative.stream", mode = READ)
    @Description("CALL algo.scc.iterative.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "nodeId, partition")
    public Stream<SCCAlgorithm.StreamResult> sccIterativeTarjanStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        final AllocationTracker tracker = AllocationTracker.create();
        final SCCAlgorithm compute = SCCAlgorithm.iterativeTarjan(graph, tracker)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(IterativeTarjan)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute();

        graph.release();

        return compute.resultStream();
    }


    // algo.scc.multistep
    @Procedure(value = "algo.scc.multistep", mode = Mode.WRITE)
    @Description("CALL algo.scc.multistep(label:String, relationship:String, {write:true, concurrency:4, cutoff:100000}) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> multistep(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        SCCResult.Builder builder = SCCResult.builder();

        ProgressTimer loadTimer = builder.timeLoad();
        AllocationTracker tracker = AllocationTracker.create();
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .load(configuration.getGraphImpl());
        loadTimer.stop();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(SCCResult.EMPTY);
        }

        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        final MultistepSCC multistep = new MultistepSCC(graph, Pools.DEFAULT,
                configuration.getConcurrency(),
                configuration.getNumber("cutoff", 100_000).intValue())
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(MultiStep)"))
                .withTerminationFlag(terminationFlag);

        builder.timeEval(multistep::compute);

        final int[] connectedComponents = multistep.getConnectedComponents();

        if (configuration.isWriteFlag()) {
            graph.release();
            multistep.release();
            builder.timeWrite(() -> {
                builder.withWrite(true);
                String partitionProperty = configuration.get(CONFIG_WRITE_PROPERTY,
                        CONFIG_OLD_WRITE_PROPERTY,
                        CONFIG_CLUSTER);
                builder.withPartitionProperty(partitionProperty);

                NodePropertyExporter
                        .of(api, graph, multistep.terminationFlag)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getWriteConcurrency())
                        .build()
                        .write(
                                partitionProperty,
                                connectedComponents,
                                Translators.OPTIONAL_INT_ARRAY_TRANSLATOR
                        );
            });
        }

        return Stream.of(builder.build(tracker, graph.nodeCount(), l -> (long) connectedComponents[((int) l)]));
    }

    // algo.scc.multistep.stream
    @Procedure(name = "algo.scc.multistep.stream", mode = READ)
    @Description("CALL algo.scc.multistep.stream(label:String, relationship:String, {write:true, concurrency:4, cutoff:100000}) YIELD " +
            "nodeId, partition")
    public Stream<SCCStreamResult> multistepStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .load(configuration.getGraphImpl());
        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }
        final MultistepSCC multistep = new MultistepSCC(graph, Pools.DEFAULT,
                configuration.getConcurrency(),
                configuration.getNumber("cutoff", 100_000).intValue())
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(MultiStep)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));
        multistep.compute();
        graph.release();
        return multistep.resultStream();
    }

    // algo.scc.forwardBackward.stream
    @Procedure(name = "algo.scc.forwardBackward.stream", mode = READ)
    @Description("CALL algo.scc.forwardBackward.stream(long startNodeId, label:String, relationship:String, {write:true, concurrency:4}) YIELD " +
            "nodeId, partition")
    public Stream<ForwardBackwardScc.Result> fwbwStream(
            @Name(value = "startNodeId", defaultValue = "0") long startNodeId,
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .load(configuration.getGraphImpl());
        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }
        final ForwardBackwardScc algo = new ForwardBackwardScc(graph, Pools.DEFAULT,
                configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(ForwardBackward)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute(graph.toMappedNodeId(startNodeId));
        graph.release();
        return algo.resultStream();
    }
}
