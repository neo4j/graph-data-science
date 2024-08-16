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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * This builder supports any type of result you could want to build - famous last words.
 * The idea is, you specialise it for use cases, and the wide parameter lists functions like a union type
 * , so not every implementation will make use of every parameter, but the abstraction covers all.
 * In-layer generic usage includes injecting the Graph, hence it is a parameter to the build method.
 * Out-layer would be injecting custom dependencies as part of a constructor in the implementing class.
 * An example could be a boolean determined at runtime, governing which bits of data to output.
 */
public interface StreamResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> {
    /**
     * You implement this and use as much or as little of the gathered data as is appropriate.
     * Plus your own injected dependencies of course.
     *
     * @param result   output from algorithm, empty when graph was empty
     */
    Stream<RESULT_TO_CALLER> build(
        Graph graph,
        GraphStore graphStore,
        CONFIGURATION configuration,
        Optional<RESULT_FROM_ALGORITHM> result
    );
}
