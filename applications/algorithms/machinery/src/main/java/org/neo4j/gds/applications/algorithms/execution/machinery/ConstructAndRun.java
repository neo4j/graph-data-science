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
package org.neo4j.gds.applications.algorithms.execution.machinery;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;

/**
 * This is a placeholder for constructing an algorithm, including its progress tracker;
 * and running it, including managing the progress tracker.
 * Obtuse name, but at least not misleading.
 */
public interface ConstructAndRun<RESULT> {
    /**
     * The parameter list here is the superset of algorithm needs.
     * Most algorithms need one or the other, not both.
     */
    RESULT constructAndRun(Graph graph, GraphStore graphStore);
}
