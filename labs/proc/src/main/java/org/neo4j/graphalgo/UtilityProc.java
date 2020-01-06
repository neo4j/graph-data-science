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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphdb.Path;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class UtilityProc extends LabsProc {

    @Procedure("algo.asPath")
    @Description("CALL algo.asPath - returns a path for the provided node ids and weights")
    public Stream<PathResult> list(
            @Name(value = "nodeIds", defaultValue = "") List<Long> nodeIds,
            @Name(value = "weights", defaultValue = "") List<Double> weights,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> rawConfig) {

        if (nodeIds.size() <= 0) {
            return Stream.empty();
        }
        long[] nodes = nodeIds.stream().mapToLong(l -> l).toArray();

        ProcedureConfiguration config = ProcedureConfiguration.create(rawConfig, getUsername());

        if (!weights.isEmpty()) {
            boolean cumulativeWeights = config.get("cumulativeWeights", true);
            if (cumulativeWeights) {
                if (nodeIds.size() != weights.size()) {
                    throw new RuntimeException(message(weights.size(), nodeIds.size(), "size of 'nodeIds'"));
                }

                return Stream.of(new PathResult(WalkPath.toPath(api, nodes, IntStream.range(0, weights.size() - 1).mapToDouble(index -> weights.get(index + 1) - weights.get(index)).toArray())));
            } else {
                if (nodeIds.size() - 1 != weights.size()) {
                    throw new RuntimeException(message(weights.size(), nodeIds.size() - 1, "size of 'nodeIds'-1"));
                }
                return Stream.of(new PathResult(WalkPath.toPath(api, nodes, weights.stream().mapToDouble(d -> d).toArray())));
            }
        } else {
            return Stream.of(new PathResult(WalkPath.toPath(api, nodes)));
        }
    }

    private String message(int actualSize, int expectedSize, String explanation) {
        return String.format("'weights' contains %d values, but %d values were expected (%s)", actualSize, expectedSize, explanation);
    }


    public static class PathResult {
        public final Path path;

        public PathResult(Path path) {
            this.path = path;
        }
    }
}
