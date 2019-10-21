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
import org.neo4j.graphalgo.api.NodePropertyContainer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.infomap.InfoMap;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class InfoMapProc {

    private static final String PAGE_RANK_PROPERTY = "pageRankProperty";
    private static final String DEFAULT_WRITE_PROPERTY_VALUE = "community";

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    private enum Setup {
        WEIGHTED, WEIGHTED_EXT_PR, UNWEIGHTED, UNWEIGHTED_EXT_PR
    }

    @Procedure(name = "algo.infoMap.stream", mode = READ)
    @Description("CALL algo.infoMap.stream('Label', 'REL', {<properties>}) YIELD nodeId, community")
    public Stream<Result> stream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration) {

        final ProcedureConfiguration config = ProcedureConfiguration.create(configuration)
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationshipType);

        final Setup setup;
        if (config.hasWeightProperty()) {
            if (config.containsKey(PAGE_RANK_PROPERTY)) setup = Setup.WEIGHTED_EXT_PR;
            else setup = Setup.WEIGHTED;
        } else {
            if (config.containsKey(PAGE_RANK_PROPERTY)) setup = Setup.UNWEIGHTED_EXT_PR;
            else setup = Setup.UNWEIGHTED;
        }

        final Graph graph;
        final InfoMap infoMap;

        // number of iterations for the pageRank computation
        final int pageRankIterations = config.getNumber("iterations", 10).intValue();
        // property name (node property) for predefined pageRanks
        final String pageRankPropertyName = config.getString(PAGE_RANK_PROPERTY, "pageRank");

        // default env
        final ProgressLogger progressLogger = ProgressLogger.wrap(log, "InfoMap");
        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        switch (setup) {

            case WEIGHTED:

                log.info("initializing weighted InfoMap with internal PageRank computation");
                String property1 = config.getWeightProperty();
                graph = new GraphLoader(db, Pools.DEFAULT)
                        .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                        .withRelationshipProperties(PropertyMapping.of(property1, 1.0))
                        .undirected()
                        .load(config.getGraphImpl());
                infoMap = InfoMap.weighted(
                        graph,
                        pageRankIterations,
                        graph,
                        config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue(),
                        config.getNumber("tau", InfoMap.TAU).doubleValue(),
                        Pools.FJ_POOL,
                        config.getConcurrency(),
                        progressLogger,
                        terminationFlag
                );
                break;

            case WEIGHTED_EXT_PR:

                log.info("initializing weighted InfoMap with predefined PageRank");
                String property = config.getWeightProperty();
                graph = new GraphLoader(db, Pools.DEFAULT)
                        .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                        .withRelationshipProperties(PropertyMapping.of(property, 1.0))
                        .withOptionalNodeProperties(PropertyMapping.of("_pr", pageRankPropertyName, 0.))
                        .undirected()
                        .load(config.getGraphImpl());
                infoMap = InfoMap.weighted(
                        graph,
                        graph.nodeProperties("_pr"),
                        graph,
                        config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue(),
                        config.getNumber("tau", InfoMap.TAU).doubleValue(),
                        Pools.FJ_POOL,
                        config.getConcurrency(),
                        progressLogger,
                        terminationFlag
                );
                break;

            case UNWEIGHTED:

                log.info("initializing unweighted InfoMap with internal PageRank computation");
                graph = new GraphLoader(db, Pools.DEFAULT)
                        .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                        .undirected()
                        .load(config.getGraphImpl());
                infoMap = InfoMap.unweighted(
                        graph,
                        pageRankIterations,
                        config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue(),
                        config.getNumber("tau", InfoMap.TAU).doubleValue(),
                        Pools.FJ_POOL,
                        config.getConcurrency(),
                        progressLogger,
                        terminationFlag
                );
                break;

            case UNWEIGHTED_EXT_PR:

                log.info("initializing unweighted InfoMap with predefined PageRank");
                graph = new GraphLoader(db, Pools.DEFAULT)
                        .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                        .withOptionalNodeProperties(PropertyMapping.of("_pr", pageRankPropertyName, 0.))
                        .undirected()
                        .load(config.getGraphImpl());
                infoMap = InfoMap.unweighted(
                        graph,
                        ((NodePropertyContainer) graph).nodeProperties("_pr")::nodeProperty,
                        config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue(),
                        config.getNumber("tau", InfoMap.TAU).doubleValue(),
                        Pools.FJ_POOL,
                        config.getConcurrency(),
                        progressLogger,
                        terminationFlag
                );
                break;

            default:
                throw new IllegalArgumentException();
        }

        final int[] communities = infoMap.compute().getCommunities();

        return IntStream.range(0, Math.toIntExact(graph.nodeCount()))
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), communities[i]));
    }


    @Procedure(value = "algo.infoMap", mode = Mode.WRITE)
    @Description("TODO")
    public Stream<InfoMapResult> writeBack(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration) {

        final ProcedureConfiguration config = ProcedureConfiguration.create(configuration)
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationshipType);


        final Setup setup;
        if (config.hasWeightProperty()) {
            if (config.containsKey(PAGE_RANK_PROPERTY)) setup = Setup.WEIGHTED_EXT_PR;
            else setup = Setup.WEIGHTED;
        } else {
            if (config.containsKey(PAGE_RANK_PROPERTY)) setup = Setup.UNWEIGHTED_EXT_PR;
            else setup = Setup.UNWEIGHTED;
        }

        final Graph graph;
        final InfoMap infoMap;

        // number of iterations for the pageRank computation
        final int pageRankIterations = config.getNumber("iterations", 10).intValue();
        // property name (node property) for predefined pageRanks
        final String pageRankPropertyName = config.getString(PAGE_RANK_PROPERTY, "pageRank");

        // env
        final InfoMapResultBuilder builder = InfoMapResult.builder();
        final ProgressLogger progressLogger = ProgressLogger.wrap(log, "InfoMap");
        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        switch (setup) {

            case WEIGHTED:

                log.info("initializing weighted InfoMap with internal PageRank computation");
                try (ProgressTimer timer = builder.timeLoad()) {

                    String property = config.getWeightProperty();
                    graph = new GraphLoader(db, Pools.DEFAULT)
                            .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                            .withRelationshipProperties(PropertyMapping.of(property, 1.0))
                            .undirected()
                            .load(config.getGraphImpl());

                    infoMap = InfoMap.weighted(
                            graph,
                            pageRankIterations,
                            graph,
                            config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue(),
                            config.getNumber("tau", InfoMap.TAU).doubleValue(),
                            Pools.FJ_POOL,
                            config.getConcurrency(),
                            progressLogger,
                            terminationFlag
                    );
                }
                break;

            case WEIGHTED_EXT_PR:

                log.info("initializing weighted InfoMap with predefined PageRank");
                try (ProgressTimer timer = builder.timeLoad()) {
                    String property = config.getWeightProperty();
                    graph = new GraphLoader(db, Pools.DEFAULT)
                            .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                            .withRelationshipProperties(PropertyMapping.of(property, 1.0))
                            .withOptionalNodeProperties(PropertyMapping.of("_pr", pageRankPropertyName, 0.))
                            .undirected()
                            .load(config.getGraphImpl());
                    infoMap = InfoMap.weighted(
                            graph,
                            graph.nodeProperties("_pr"),
                            graph,
                            config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue(),
                            config.getNumber("tau", InfoMap.TAU).doubleValue(),
                            Pools.FJ_POOL,
                            config.getConcurrency(),
                            progressLogger,
                            terminationFlag
                    );
                }
                break;

            case UNWEIGHTED:

                log.info("initializing unweighted InfoMap with internal PageRank computation");
                try (ProgressTimer timer = builder.timeLoad()) {
                    graph = new GraphLoader(db, Pools.DEFAULT)
                            .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                            .undirected()
                            .load(config.getGraphImpl());
                    infoMap = InfoMap.unweighted(
                            graph,
                            pageRankIterations,
                            config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue(),
                            config.getNumber("tau", InfoMap.TAU).doubleValue(),
                            Pools.FJ_POOL,
                            config.getConcurrency(),
                            progressLogger,
                            terminationFlag
                    );
                }
                break;

            case UNWEIGHTED_EXT_PR:

                log.info("initializing unweighted InfoMap with predefined PageRank");
                try (ProgressTimer timer = builder.timeLoad()) {
                    graph = new GraphLoader(db, Pools.DEFAULT)
                            .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                            .withOptionalNodeProperties(PropertyMapping.of("_pr", pageRankPropertyName, 0.))
                            .undirected()
                            .load(config.getGraphImpl());
                    infoMap = InfoMap.unweighted(
                            graph,
                            graph.nodeProperties("_pr"),
                            config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue(),
                            config.getNumber("tau", InfoMap.TAU).doubleValue(),
                            Pools.FJ_POOL,
                            config.getConcurrency(),
                            progressLogger,
                            terminationFlag
                    );
                }
                break;

            default:
                throw new IllegalArgumentException();
        }

        // eval
        builder.timeEval(infoMap::compute);
        // result
        builder.withCommunityCount(infoMap.getCommunityCount());
        builder.withNodeCount(graph.nodeCount());
        builder.withIterations(infoMap.getIterations());

        if (config.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                Exporter.of(db, graph)
                        .withLog(log)
                        .build()
                        .write(config.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE),
                                infoMap.getCommunities(),
                                Translators.INT_ARRAY_TRANSLATOR);
            }
        }


        return Stream.of(builder.build());

    }


    /**
     * result object
     */
    public static final class Result {

        public final long nodeId;
        public final long community;

        public Result(long id, long community) {
            this.nodeId = id;
            this.community = community;
        }
    }

    public static class InfoMapResultBuilder extends AbstractResultBuilder<InfoMapResult> {

        private long nodeCount = 0;
        private long communityCount = 0;
        private long iterations = 1;

        public InfoMapResultBuilder withIterations(long iterations) {
            this.iterations = iterations;
            return this;
        }

        public InfoMapResultBuilder withCommunityCount(long setCount) {
            this.communityCount = setCount;
            return this;
        }

        public InfoMapResultBuilder withNodeCount(long nodes) {
            this.nodeCount = nodes;
            return this;
        }

        public InfoMapResult build() {
            return new InfoMapResult(loadDuration, evalDuration, writeDuration, nodeCount, iterations, communityCount);
        }
    }

    public static class InfoMapResult {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long nodeCount;
        public final long iterations;
        public final long communityCount;

        private InfoMapResult(long loadMillis, long computeMillis, long writeMillis, long nodeCount, long iterations, long communityCount) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodeCount = nodeCount;
            this.iterations = iterations;
            this.communityCount = communityCount;
        }

        public static InfoMapResultBuilder builder() {
            return new InfoMapResultBuilder();
        }
    }

}
