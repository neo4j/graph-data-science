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
package org.neo4j.gds.community.validation;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;

import java.util.Collection;
import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class ApproxMaxKCutValidation extends GraphStoreValidation  {

    private final List<Long> minCommunitySizes;

    public ApproxMaxKCutValidation(List<Long> minCommunitySizes) {
        this.minCommunitySizes = minCommunitySizes;
    }


    @Override
    protected void validateAlgorithmRequirements(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        validateMinCommunitySizesSum(graphStore);
    }

    void validateMinCommunitySizesSum(GraphStore graphStore){
        long minCommunitySizesSum = minCommunitySizes.stream().mapToLong(Long::valueOf).sum();
        long halfNodeCount = graphStore.nodeCount() / 2;
        if (minCommunitySizesSum > halfNodeCount) {
            throw new IllegalArgumentException(formatWithLocale(
                "The sum of min community sizes is larger than half of the number of nodes in the graph: %d > %d",
                minCommunitySizesSum,
                halfNodeCount
            ));
        }
    }

}
