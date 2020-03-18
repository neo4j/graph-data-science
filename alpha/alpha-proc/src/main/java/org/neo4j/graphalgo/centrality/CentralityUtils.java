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
package org.neo4j.graphalgo.centrality;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.config.WritePropertyConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.CentralityResult;
import org.neo4j.graphalgo.results.CentralityScore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class CentralityUtils {

    private CentralityUtils() {}

    public static <R> void write(
        GraphDatabaseAPI api,
        Log log,
        IdMapping graph,
        TerminationFlag terminationFlag,
        CentralityResult result,
        WritePropertyConfig configuration,
        AbstractResultBuilder<CentralityScore.Stats> statsBuilder
    ) {
        log.debug("Writing results");
        String propertyName = configuration.writeProperty();
        try(ProgressTimer ignore = ProgressTimer.start(statsBuilder::withWriteMillis)) {
            NodePropertyExporter exporter = NodePropertyExporter
                .of(api, graph, terminationFlag)
                .withLog(log)
                .parallel(Pools.DEFAULT, configuration.writeConcurrency())
                .build();
            result.export(propertyName, exporter);
        }
        statsBuilder.withConfig(configuration);
    }

    public static Stream<CentralityScore> streamResults(Graph graph, CentralityResult scores) {
        return LongStream.range(0, graph.nodeCount())
            .mapToObj(i -> {
                    final long nodeId = graph.toOriginalNodeId(i);
                    return new CentralityScore(
                        nodeId,
                        scores.score(i)
                    );
                }
            );
    }
}
