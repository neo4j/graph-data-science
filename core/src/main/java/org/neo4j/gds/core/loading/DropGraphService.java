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
package org.neo4j.gds.core.loading;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class DropGraphService {
    private final GraphStoreCatalogService graphStoreCatalogService;

    public DropGraphService(GraphStoreCatalogService graphStoreCatalogService) {
        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    List<GraphStoreWithConfig> compute(
        Iterable<String> graphNames,
        boolean shouldFailIfMissing,
        DatabaseId databaseId,
        User operator,
        Optional<String> usernameOverride
    ) {
        var request = ImmutableCatalogRequest.of(
            databaseId.databaseName(),
            operator.getUsername(),
            usernameOverride,
            operator.isAdmin()
        );

        if (shouldFailIfMissing) validateGraphsExist(request, graphNames);

        return dropGraphs(request, graphNames, shouldFailIfMissing);
    }

    @NotNull
    private List<GraphStoreWithConfig> dropGraphs(
        CatalogRequest request,
        Iterable<String> graphNames,
        boolean shouldFailIfMissing
    ) {
        var results = new LinkedList<GraphStoreWithConfig>();
        for (String graphName : graphNames) {
            var result = graphStoreCatalogService.removeGraph(
                request,
                graphName,
                shouldFailIfMissing
            );
            results.add(result);
        }
        return results;
    }

    private void validateGraphsExist(CatalogRequest request, Iterable<String> graphNames) {
        List<Pair<String, NoSuchElementException>> failures = new LinkedList<>();
        graphNames.forEach(graphName -> {
                try {
                    graphStoreCatalogService.get(request, graphName);
                } catch (NoSuchElementException e) {
                    failures.add(Pair.of(graphName, e));
                }
            }
        );

        if (failures.isEmpty()) return;

        throw missingGraphs(failures, request.databaseName());
    }

    private NoSuchElementException missingGraphs(
        List<Pair<String, NoSuchElementException>> missingGraphs,
        String databaseName
    ) {
        if (missingGraphs.size() == 1) {
            return missingGraphs.get(0).getRight();
        }

        var message = new StringBuilder("The graphs");
        for (int i = 0; i < missingGraphs.size() - 1; i++) {
            message.append(" `").append(missingGraphs.get(i).getLeft()).append("`,");
        }
        message
            .append(" and `")
            .append(missingGraphs.get(missingGraphs.size() - 1).getLeft())
            .append("` do not exist on database `")
            .append(databaseName)
            .append("`.");

        var exception = new NoSuchElementException(message.toString());
        for (var missingGraph : missingGraphs) {
            exception.addSuppressed(missingGraph.getRight());
        }
        return exception;
    }
}
