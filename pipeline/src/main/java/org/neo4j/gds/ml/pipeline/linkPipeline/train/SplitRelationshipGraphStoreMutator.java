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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.ml.splitting.EdgeSplitter;
import org.neo4j.gds.ml.splitting.SplitRelationshipsBaseConfig;
import org.neo4j.values.storable.NumberType;

import java.util.Optional;

public final class SplitRelationshipGraphStoreMutator {

    private SplitRelationshipGraphStoreMutator() {}

    public static void mutate(GraphStore graphStore, EdgeSplitter.SplitResult splitResult, SplitRelationshipsBaseConfig config) {
        graphStore.addRelationshipType(
            config.remainingRelationshipType(),
            config.relationshipWeightProperty(),
            Optional.of(NumberType.FLOATING_POINT),
            splitResult.remainingRels()
        );

        graphStore.addRelationshipType(
            config.holdoutRelationshipType(),
            Optional.of(EdgeSplitter.RELATIONSHIP_PROPERTY),
            Optional.of(NumberType.INTEGRAL),
            splitResult.selectedRels()
        );
    }
}
