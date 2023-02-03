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
package org.neo4j.gds.test;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public class TestAlgorithm extends Algorithm<TestAlgorithmResult> {

    private final Graph graph;
    private final boolean throwInCompute;

    public TestAlgorithm(
        Graph graph,
        ProgressTracker progressTracker,
        boolean throwInCompute
    ) {
        super(progressTracker);
        this.graph = graph;
        this.throwInCompute = throwInCompute;
    }

    @Override
    public TestAlgorithmResult compute() {
        progressTracker.beginSubTask();

        if (throwInCompute) {
            throw new IllegalStateException("boo");
        }

        progressTracker.endSubTask();

        return new TestAlgorithmResult(graph.relationshipCount());
    }
}
