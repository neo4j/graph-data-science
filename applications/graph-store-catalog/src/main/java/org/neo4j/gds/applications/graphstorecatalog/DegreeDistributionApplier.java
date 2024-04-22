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
package org.neo4j.gds.applications.graphstorecatalog;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class DegreeDistributionApplier {
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final DegreeDistributionService degreeDistributionService;

    DegreeDistributionApplier(
        GraphStoreCatalogService graphStoreCatalogService,
        DegreeDistributionService degreeDistributionService
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.degreeDistributionService = degreeDistributionService;
    }

    List<Pair<GraphStoreWithConfig, Map<String, Object>>> process(
        Collection<Pair<GraphProjectConfig, GraphStore>> graphEntries,
        boolean includeDegreeDistribution,
        TerminationFlag terminationFlag
    ) {
        return graphEntries.stream().map(configAndStore -> {
            var graphStoreWithConfig = new GraphStoreWithConfig(configAndStore.getValue(), configAndStore.getKey());
            var degreeDistribution = getOrCreateDegreeDistribution(
                includeDegreeDistribution,
                graphStoreWithConfig,
                terminationFlag
            );
            return Pair.of(graphStoreWithConfig, degreeDistribution);
        }).collect(Collectors.toList());
    }

    /**
     * This is get if cached; create if does not exist; or return null depending on the flag
     */
    private Map<String, Object> getOrCreateDegreeDistribution(
        boolean includeDegreeDistribution,
        GraphStoreWithConfig graphStoreWithConfig,
        TerminationFlag terminationFlag
    ) {
        if (!includeDegreeDistribution) return null;

        // we shall eventually have microtypes, by hook or by crook
        var usernameAsString = graphStoreWithConfig.config().username();
        var user = new User(usernameAsString, false);
        var graphNameAsString = graphStoreWithConfig.config().graphName();
        var graphName = GraphName.parse(graphNameAsString);

        var maybeDegreeDistribution = graphStoreCatalogService.getDegreeDistribution(
            user,
            graphStoreWithConfig.graphStore().databaseInfo().databaseId(),
            graphName
        );

        return maybeDegreeDistribution.orElseGet(() -> {
            var histogram = degreeDistributionService.compute(graphStoreWithConfig.graphStore(), terminationFlag);

            cacheHistogram(graphStoreWithConfig, user, graphName, histogram);

            return histogram;
        });
    }

    // Cache the computed degree distribution in the Catalog
    private void cacheHistogram(
        GraphStoreWithConfig graphStoreWithConfig,
        User user,
        GraphName graphName,
        Map<String, Object> histogram
    ) {
        graphStoreCatalogService.setDegreeDistribution(
            user,
            graphStoreWithConfig.graphStore().databaseInfo().databaseId(),
            graphName,
            histogram
        );
    }
}
