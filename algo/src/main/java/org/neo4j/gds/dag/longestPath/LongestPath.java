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
package org.neo4j.gds.dag.longestPath;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortFactory;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortStreamConfig;

/*
 * Uses topological sort to calculate the longest path for the targets
 */
public class LongestPath extends Algorithm<TopologicalSortResult> {
    private final Graph graph;
    private final LongestPathBaseConfig config;

    protected LongestPath(Graph graph, LongestPathBaseConfig config, ProgressTracker progressTracker) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
    }

    @Override
    public TopologicalSortResult compute() {

        var topologicalSortConfigMap =
            CypherMapWrapper
                .create(config.toMap())
                .withBoolean("computeMaxDistanceFromSource", true);

        return new TopologicalSortFactory().build(
            graph,
            TopologicalSortStreamConfig.of(topologicalSortConfigMap),
            progressTracker
        ).compute();
    }
}
