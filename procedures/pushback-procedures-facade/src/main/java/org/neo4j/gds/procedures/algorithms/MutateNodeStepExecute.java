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
package org.neo4j.gds.procedures.algorithms;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.utils.ProgressTimer;

import java.util.concurrent.atomic.AtomicLong;

public final class MutateNodeStepExecute {

    private MutateNodeStepExecute() {}

    public static <RESULT_FROM_ALGORITHM> MutateNodePropertyMetadata executeMutateNodePropertyStep(
        MutateStep<RESULT_FROM_ALGORITHM, NodePropertiesWritten> mutateStep,
        Graph graph,
        GraphStore graphStore,
        RESULT_FROM_ALGORITHM algorithmResult
    ) {
        NodePropertiesWritten nodePropertiesWritten;
        var mutateMillis = new AtomicLong();
        try (var ignored = ProgressTimer.start(mutateMillis::set)) {
            nodePropertiesWritten = mutateStep.execute(
                graph,
                graphStore,
                algorithmResult
            );
        }
        return new MutateNodePropertyMetadata(nodePropertiesWritten, mutateMillis.get());
    }


    public record MutateNodePropertyMetadata(NodePropertiesWritten nodePropertiesWritten, long mutateMillis){}
}
