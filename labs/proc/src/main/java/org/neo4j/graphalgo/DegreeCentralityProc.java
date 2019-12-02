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
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.degree.DegreeCentrality;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.CentralityScore;
import org.neo4j.graphalgo.impl.utils.CentralityUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.ProcedureConstants.CYPHER_QUERY_KEY;
import static org.neo4j.procedure.Mode.READ;

public final class DegreeCentralityProc extends LabsProc {

    public static final String DEFAULT_SCORE_PROPERTY = "degree";
    public static final String CONFIG_WEIGHT_KEY = "weightProperty";

    @Procedure(value = "algo.degree", mode = Mode.WRITE)
    @Description("CALL algo.degree(label:String, relationship:String, " +
                 "{ weightProperty: null, write: true, writeProperty:'degree', concurrency:4}) " +
                 "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty" +
                 " - calculates degree centrality and potentially writes back")
    public Stream<CentralityScore.Stats> degree(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        final String weightPropertyKey = configuration.getString(CONFIG_WEIGHT_KEY, null);

        CentralityScore.Stats.Builder statsBuilder = new CentralityScore.Stats.Builder();
        AllocationTracker tracker = AllocationTracker.create();
        Direction direction = getDirection(configuration);
        final Graph graph = load(
                label,
                relationship,
                tracker,
                configuration.getGraphImpl(),
                statsBuilder,
                configuration,
                weightPropertyKey,
                direction);

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(statsBuilder.build());
        }

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        CentralityResult scores = evaluate(
                graph,
                tracker,
                terminationFlag,
                configuration,
                statsBuilder,
                weightPropertyKey
        );

        logMemoryUsage(tracker);

        CentralityUtils.write(
                api,
                log,
                graph,
                terminationFlag,
                scores,
                configuration,
                statsBuilder,
                DEFAULT_SCORE_PROPERTY);

        return Stream.of(statsBuilder.build());
    }

    private Direction getDirection(ProcedureConfiguration configuration) {
        String graphName = configuration.getGraphName(ProcedureConstants.GRAPH_IMPL_DEFAULT);
        Direction direction = configuration.getDirection(Direction.INCOMING);
        return CYPHER_QUERY_KEY.equals(graphName) ? Direction.OUTGOING : direction;
    }

    @Procedure(name = "algo.degree.stream", mode = READ)
    @Description("CALL algo.degree.stream(label:String, relationship:String, " +
                 "{weightProperty: null, concurrency:4}) " +
                 "YIELD node, score - calculates degree centrality and streams results")
    public Stream<CentralityScore> degreeStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        final String weightPropertyKey = configuration.getString(CONFIG_WEIGHT_KEY, null);

        CentralityScore.Stats.Builder statsBuilder = new CentralityScore.Stats.Builder();
        Direction direction = getDirection(configuration);
        AllocationTracker tracker = AllocationTracker.create();
        final Graph graph = load(
                label,
                relationship,
                tracker,
                configuration.getGraphImpl(),
                statsBuilder,
                configuration,
                weightPropertyKey,
                direction);

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        CentralityResult scores = evaluate(
                graph,
                tracker,
                terminationFlag,
                configuration,
                statsBuilder,
                weightPropertyKey
        );

        logMemoryUsage(tracker);

        return CentralityUtils.streamResults(graph, scores);
    }

    private void logMemoryUsage(AllocationTracker tracker) {
        log.info("Degree Centrality: overall memory usage: %s", tracker.getUsageString());
    }

    private Graph load(
            String label,
            String relationship,
            AllocationTracker tracker,
            Class<? extends GraphFactory> graphFactory,
            CentralityScore.Stats.Builder statsBuilder,
            ProcedureConfiguration configuration,
            String weightPropertyKey,
            Direction direction) {
        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .withRelationshipProperties(PropertyMapping.of(
                        weightPropertyKey,
                        configuration.getWeightPropertyDefaultValue(0.0)))
                .withReducedRelationshipLoading(direction);

        try (ProgressTimer timer = statsBuilder.timeLoad()) {
            Graph graph = graphLoader.load(graphFactory);
            statsBuilder.withNodeCount(graph.nodeCount());
            return graph;
        }
    }

    private CentralityResult evaluate(
            Graph graph,
            AllocationTracker tracker,
            TerminationFlag terminationFlag,
            ProcedureConfiguration configuration,
            CentralityScore.Stats.Builder statsBuilder,
            String weightPropertyKey) {

        Direction computeDirection = getDirection(configuration) == Direction.BOTH
                ? Direction.OUTGOING
                : getDirection(configuration);

        DegreeCentrality algo = new DegreeCentrality(
                graph,
                Pools.DEFAULT,
                configuration.concurrency(),
                computeDirection,
                weightPropertyKey != null,
                tracker
        );
        statsBuilder.timeEval(algo::compute);
        Algorithm<?, ?> algorithm = algo.algorithm();
        algorithm.withTerminationFlag(terminationFlag);

        final CentralityResult result = algo.result();
        algo.algorithm().release();
        graph.release();
        return result;
    }


}
