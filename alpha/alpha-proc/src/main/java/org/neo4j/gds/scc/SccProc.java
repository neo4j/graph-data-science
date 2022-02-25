/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.scc;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NodePropertiesWriter;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.scc.SccAlgorithm;
import org.neo4j.gds.impl.scc.SccConfig;

public abstract class SccProc<PROC_RESULT> extends NodePropertiesWriter<SccAlgorithm, HugeLongArray, SccConfig, PROC_RESULT> {

    protected static final String DESCRIPTION =
        "The SCC algorithm finds sets of connected nodes in an directed graph, " +
        "where all nodes in the same set form a connected component.";

    @Override
    protected SccConfig newConfig(String username, CypherMapWrapper config) {
        return SccConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<SccAlgorithm, SccConfig> algorithmFactory() {
        return new GraphAlgorithmFactory<>() {
            @Override
            public String taskName() {
                return "Scc";
            }

            @Override
            public SccAlgorithm build(
                Graph graph,
                SccConfig configuration,
                ProgressTracker progressTracker
            ) {
                return new SccAlgorithm(
                    graph,
                    progressTracker
                );
            }
        };
    }
}
