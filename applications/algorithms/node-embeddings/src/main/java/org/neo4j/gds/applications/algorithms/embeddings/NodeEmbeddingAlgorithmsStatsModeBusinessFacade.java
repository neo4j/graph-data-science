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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultRenderer;
import org.neo4j.gds.embeddings.fastrp.FastRPBaseConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;

import java.util.Optional;

public class NodeEmbeddingAlgorithmsStatsModeBusinessFacade {
    private final InstrumentedNodeEmbeddingAlgorithms instrumentedAlgorithms;
    private final Synchroniser synchroniser;

    NodeEmbeddingAlgorithmsStatsModeBusinessFacade(
        InstrumentedNodeEmbeddingAlgorithms instrumentedAlgorithms,
        Synchroniser synchroniser
    ) {
        this.instrumentedAlgorithms = instrumentedAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> RESULT fastRP(
        GraphName graphName,
        FastRPBaseConfig configuration,
        StatsResultBuilder<FastRPResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedAlgorithms.fastRP(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }
}
