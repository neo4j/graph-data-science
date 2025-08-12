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
package org.neo4j.gds.cliqueCounting;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public final class CliqueCountingTaskFactory {

    private CliqueCountingTaskFactory() {}

    private static long volume(Graph graph, CliqueCountingParameters parameters) {
        return parameters.subcliques().isEmpty() ? graph.nodeCount() : parameters.subcliques().size();
    }

    public static Task createTask(Graph graph, CliqueCountingParameters parameters) {
        return Tasks.leaf(AlgorithmLabel.CliqueCounting.asString(), volume(graph, parameters));
    }



}
