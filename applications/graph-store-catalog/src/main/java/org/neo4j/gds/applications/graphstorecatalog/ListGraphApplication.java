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
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.utils.TerminationFlag;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ListGraphApplication {
    private final GraphListingService graphListingService;
    private final DegreeDistributionApplier degreeDistributionApplier;

    ListGraphApplication(GraphListingService graphListingService, DegreeDistributionApplier degreeDistributionApplier) {
        this.graphListingService = graphListingService;
        this.degreeDistributionApplier = degreeDistributionApplier;
    }

    public static ListGraphApplication create(GraphStoreCatalogService graphStoreCatalogService) {
        var graphListingService = new GraphListingService(graphStoreCatalogService);
        var degreeDistributionApplier = new DegreeDistributionApplier(graphStoreCatalogService,
            new DegreeDistributionService()
        );

        return new ListGraphApplication(graphListingService, degreeDistributionApplier);
    }

    public List<Pair<GraphStoreWithConfig, Map<String, Object>>> list(
        User user,
        Optional<GraphName> graphName,
        boolean includeDegreeDistribution,
        TerminationFlag terminationFlag
    ) {
        var graphEntries = graphListingService.listGraphs(user);

        if (graphName.isPresent()) {
            // we should only list the provided graph
            graphEntries = graphEntries.stream()
                .filter(e -> e.getKey().graphName().equals(graphName.get().getValue()))
                .collect(Collectors.toList());
        }

        return degreeDistributionApplier.process(graphEntries, includeDegreeDistribution, terminationFlag);
    }
}
