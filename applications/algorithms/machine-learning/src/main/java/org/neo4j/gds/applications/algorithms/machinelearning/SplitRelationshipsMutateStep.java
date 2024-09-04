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
package org.neo4j.gds.applications.algorithms.machinelearning;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.ml.splitting.EdgeSplitter;

class SplitRelationshipsMutateStep implements MutateStep<EdgeSplitter.SplitResult, RelationshipsWritten> {
    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        EdgeSplitter.SplitResult result
    ) {
        var selectedRelationships = result.selectedRels().build();
        var remainingRelationships = result.remainingRels().build();

        graphStore.addRelationshipType(remainingRelationships);
        graphStore.addRelationshipType(selectedRelationships);

        var holdoutWritten = selectedRelationships.topology().elementCount();
        var remainingWritten = remainingRelationships.topology().elementCount();

        return new RelationshipsWritten(holdoutWritten + remainingWritten);
    }
}
