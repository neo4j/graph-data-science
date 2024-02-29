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
package org.neo4j.gds.approxmaxkcut.config;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutParameters;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.SingleThreadedRandomSeedConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface ApproxMaxKCutBaseConfig extends AlgoBaseConfig,
    RelationshipWeightConfig,
    SingleThreadedRandomSeedConfig {

    @Configuration.IntegerRange(min = 2, max = Byte.MAX_VALUE)
    default byte k() {
        return 2;
    }

    @Configuration.IntegerRange(min = 1)
    default int iterations() {
        return 8;
    }

    @Configuration.IntegerRange(min = 0)
    default int vnsMaxNeighborhoodOrder() {
        return 0;
    }

    // Min k-cut capabilities not exposed in API yet.
    @Configuration.Ignore
    default boolean minimize() {
        return false;
    }

    // Min k-cut capabilities not exposed in API yet.
    @Configuration.Ignore
    default int batchSize() {
        return 10_000;
    }

    // Min k-cut capabilities not exposed in API yet.
    @Configuration.Ignore
    default List<Long> minCommunitySizes() {
        if (minimize()) {
            return Collections.nCopies(k(), 1L);
        }
        return Collections.nCopies(k(), 0L);
    }

    @Configuration.Check
    default void validateMinCommunitySizes() {
        if (minCommunitySizes().size() != k()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Configuration parameter 'minCommunitySizes' must be of length 'k' which is equal to %d, but got a %d length list",
                k(),
                minCommunitySizes().size()
            ));
        }

        long minMinSize = minimize() ? 1L : 0L;
        if (minCommunitySizes().stream().anyMatch(size -> size < minMinSize)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Configuration parameter 'minCommunitySizes' must only have entries at least as large as %d when %s",
                minMinSize,
                minimize() ? "minimizing" : "maximizing"
            ));
        }
    }

    @Configuration.GraphStoreValidationCheck
    default void validateMinCommunitySizesSum(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        long minCommunitySizesSum = minCommunitySizes().stream().mapToLong(Long::valueOf).sum();
        long halfNodeCount = graphStore.nodeCount() / 2;
        if (minCommunitySizesSum > halfNodeCount) {
            throw new IllegalArgumentException(formatWithLocale(
                "The sum of min community sizes is larger than half of the number of nodes in the graph: %d > %d",
                minCommunitySizesSum,
                halfNodeCount
            ));
        }
    }

    @Configuration.Ignore
    default ApproxMaxKCutParameters toParameters() {
        return new ApproxMaxKCutParameters(
            k(),
            iterations(),
            vnsMaxNeighborhoodOrder(),
            concurrency(),
            batchSize(),
            randomSeed(),
            minCommunitySizes(),
            hasRelationshipWeightProperty(),
            minimize()
        );
    }
}
