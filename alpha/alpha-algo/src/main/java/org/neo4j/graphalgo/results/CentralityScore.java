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
package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.config.WritePropertyConfig;
import org.neo4j.graphalgo.result.AbstractResultBuilder;

public class CentralityScore {

    public final long nodeId;
    public final Double score;

    public CentralityScore(long nodeId, final Double score) {
        this.nodeId = nodeId;
        this.score = score;
    }

    // TODO: return number of relationships as well
    //  the Graph API doesn't expose this value yet
    public static final class Stats {
        public final long nodes, createMillis, computeMillis, writeMillis;
        public final String writeProperty;

        public Stats(
                long nodes,
                long createMillis,
                long computeMillis,
                long writeMillis,
                String writeProperty) {
            this.nodes = nodes;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.writeProperty = writeProperty;
        }

        public static final class Builder extends AbstractResultBuilder<Stats> {

            public CentralityScore.Stats build() {
                return new CentralityScore.Stats(
                    nodeCount,
                    createMillis,
                    computeMillis,
                    writeMillis,
                    ((WritePropertyConfig) config).writeProperty()
                );
            }
        }
    }
}
