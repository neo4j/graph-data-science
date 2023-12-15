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
package org.neo4j.gds.applications.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;

import java.util.Optional;

/**
 * This builder gathers data as part of algorithm processing. Timings and such.
 * You specialise it for use cases, but a lot of what it needs is generic,
 * and it is part of algorithm processing instrumentation.
 * In-layer generic usage includes injecting the Graph, hence it is a parameter to the build method.
 * Out-layer would be injecting custom dependencies as part of the constructor.
 * And the whole build method is bespoke, of course.
 * This class is generic in the union type sense, it has fields and accessors for lots of stuff,
 * where any given usage probably won't need all of them.
 */
public abstract class ResultBuilder<RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> {
    // timings
    protected long preProcessingMillis;
    protected long computeMillis;
    protected long postProcessingMillis = -1; // mutate or write timing

    // union type: zero or more of these get populated by your own hooks
    protected long nodeCount;
    protected long nodePropertiesWritten;
    protected long relationshipsWritten;

    public void withPreProcessingMillis(long preProcessingMillis) {
        this.preProcessingMillis = preProcessingMillis;
    }

    public void withComputeMillis(long computeMillis) {
        this.computeMillis = computeMillis;
    }

    public void withPostProcessingMillis(long postProcessingMillis) {
        this.postProcessingMillis = postProcessingMillis;
    }

    public void withNodeCount(long nodeCount) {
        this.nodeCount = nodeCount;
    }

    public void withNodePropertiesWritten(long nodePropertiesWritten) {
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    public void withRelationshipsWritten(long relationshipPropertiesWritten) {
        this.relationshipsWritten = relationshipPropertiesWritten;
    }

    /**
     * You implement this and use as much or as little of the gathered data as is appropriate.
     * Plus your own injected dependencies of course.
     *
     * @param result empty when graph was empty
     */
    public abstract RESULT_TO_CALLER build(
        Graph graph,
        GraphStore graphStore,
        Optional<RESULT_FROM_ALGORITHM> result
    );
}
