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
package org.neo4j.graphalgo.impl.utils;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.results.AbstractWriteBuilder;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.CentralityScore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class CentralityUtils {
    public static void write(GraphDatabaseAPI api, Log log, Graph graph, TerminationFlag terminationFlag,
                             CentralityResult result, ProcedureConfiguration configuration,
                             AbstractWriteBuilder statsBuilder,
                             String defaultScoreProperty) {
        if (configuration.isWriteFlag(true)) {
            log.debug("Writing results");
            String propertyName = configuration.getWriteProperty(defaultScoreProperty);
            try (ProgressTimer timer = statsBuilder.timeWrite()) {
                Exporter exporter = Exporter
                        .of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                        .build();
                result.export(propertyName, exporter);
            }
            statsBuilder.withWrite(true).withProperty(propertyName);
        } else {
            statsBuilder.withWrite(false);
        }
    }

    public static Stream<CentralityScore> streamResults(Graph graph, CentralityResult scores) {
            return LongStream.range(0, graph.nodeCount())
                    .mapToObj(i -> {
                        final long nodeId = graph.toOriginalNodeId(i);
                        return new CentralityScore(
                                nodeId,
                                scores.score(i)
                        );
                    });
    }

    public static void normalizeArray(double[][] partitions, Function<Double, Double> normalizationFunction) {
        for (double[] partition : partitions) {
            for (int j = 0; j < partition.length; j++) {
                partition[j] = normalizationFunction.apply(partition[j]);
            }
        }
    }
}
