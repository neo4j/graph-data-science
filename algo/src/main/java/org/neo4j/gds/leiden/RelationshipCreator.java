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
package org.neo4j.gds.leiden;

import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;

final class RelationshipCreator implements Runnable {

    private final double propertyScale;
    private final RelationshipsBuilder relationshipsBuilder;

    private final HugeLongArray communities;

    private final RelationshipIterator relationshipIterator;

    private final Partition partition;


    RelationshipCreator(
        HugeLongArray communities,
        Partition partition,
        RelationshipsBuilder relationshipsBuilder,
        RelationshipIterator relationshipIterator,
        double propertyScale
    ) {
        this.propertyScale = propertyScale;
        this.relationshipsBuilder = relationshipsBuilder;
        this.communities = communities;
        this.relationshipIterator = relationshipIterator;
        this.partition = partition;
    }

    @Override
    public void run() {
        partition.consume(nodeId -> {
            long communityId = communities.get(nodeId);
            relationshipIterator.forEachRelationship(nodeId, 1.0, (source, target, property) -> {
                // do not allow self-loops
                if (communityId != communities.get(target)) {
                    relationshipsBuilder.add(communityId, communities.get(target), property * propertyScale);
                }
                return true;
            });
        });
    }
}
