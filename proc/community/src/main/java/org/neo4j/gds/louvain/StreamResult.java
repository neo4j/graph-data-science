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
package org.neo4j.gds.louvain;

import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public final class StreamResult {
    public final long nodeId;
    public final long communityId;
    public final List<Long> intermediateCommunityIds;

    StreamResult(long nodeId, @Nullable long[] intermediateCommunityIds, long communityId) {
        this.nodeId = nodeId;
        this.intermediateCommunityIds = intermediateCommunityIds == null ? null : Arrays
            .stream(intermediateCommunityIds)
            .boxed()
            .collect(Collectors.toList());
        this.communityId = communityId;
    }
}
