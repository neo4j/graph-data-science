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
package org.neo4j.gds.algorithms;

import java.util.Map;

import static java.util.Collections.emptyMap;

public final class LabelPropagationSpecificFields implements CommunityStatisticsSpecificFields {

    public static final LabelPropagationSpecificFields EMPTY =
        new LabelPropagationSpecificFields(0L, false, 0L, emptyMap());

    private final long ranIterations;
    private final boolean didConverge;
    private final long communityCount;
    private final Map<String, Object> communityDistribution;

    public static LabelPropagationSpecificFields from(
        long ranIterations,
        boolean didConverge,
        long componentCount,
        Map<String, Object> componentDistribution
    ) {

        return new LabelPropagationSpecificFields(
            ranIterations,
            didConverge,
            componentCount,
            componentDistribution
        );
    }

    private LabelPropagationSpecificFields(
        long ranIterations,
        boolean didConverge, long communityCount,
        Map<String, Object> communityDistribution
    ) {
        this.didConverge = didConverge;
        this.communityCount = communityCount;
        this.communityDistribution = communityDistribution;
        this.ranIterations = ranIterations;
    }

    public long ranIterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    @Override
    public long communityCount() {
        return communityCount;
    }

    @Override
    public Map<String, Object> communityDistribution() {
        return communityDistribution;
    }

}
