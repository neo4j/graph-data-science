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
package org.neo4j.gds.catalog;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class GraphDropProc extends CatalogProc {

    private static final String DESCRIPTION = "Drops a named graph from the catalog and frees up the resources it occupies.";

    @Procedure(name = "gds.graph.drop", mode = READ)
    @Description(DESCRIPTION)
    public Stream<GraphInfo> drop(
        @Name(value = "graphName") Object graphName,
        @Name(value = "failIfMissing", defaultValue = "true") boolean failIfMissing,
        @Name(value = "dbName", defaultValue = "") String dbName,
        @Name(value = "username", defaultValue = "") String username
    ) {
        ProcPreconditions.check();

        final List<String> graphNames;
        if (graphName instanceof Collection<?>) {
            Collection<?> names = (Collection<?>) graphName;
            graphNames = new ArrayList<>(names.size());
            int index = 0;
            for (Object name : names) {
                if (name instanceof String || name == null) {
                    graphNames.add(validateGraphName((String) name));
                } else {
                    throw typeMismatch(name, index);
                }
                index++;
            }
        } else if (graphName instanceof String || graphName == null) {
            graphNames = List.of(validateGraphName((String) graphName));
        } else {
            throw typeMismatch(graphName, -1);
        }

        var config = GraphDropConfig.of(failIfMissing, dbName, username);
        var request = ImmutableCatalogRequest.of(
            config.databaseName().orElseGet(databaseId()::databaseName),
            username(),
            config.catalogUser(),
            isGdsAdmin()
        );

        if (failIfMissing) {
            var missingGraphs = graphNames.stream().flatMap(name -> {
                try {
                    // get the graph to check if it exists
                    GraphStoreCatalog.get(request, name);
                    return Stream.empty();
                } catch (NoSuchElementException missing) {
                    return Stream.of(new MissingGraph(name, missing));
                }
            }).collect(Collectors.toList());
            if (!missingGraphs.isEmpty()) {
                throw missingGraphs(missingGraphs, request.databaseName());
            }
        }

        var result = Stream.<GraphInfo>builder();
        for (String name : graphNames) {
            GraphStoreCatalog.remove(
                request,
                name,
                graphStoreWithConfig -> result.add(
                    GraphInfo.withoutMemoryUsage(
                        graphStoreWithConfig.config(),
                        graphStoreWithConfig.graphStore()
                    )
                ),
                failIfMissing
            );
        }

        return result.build();
    }

    private IllegalArgumentException typeMismatch(@NotNull Object invalid, int index) {
        return new IllegalArgumentException(formatWithLocale(
            "Type mismatch%s: expected String but was %s.",
            index >= 0 ? (" at index " + index) : "",
            invalid.getClass().getSimpleName()
        ));
    }

    private NoSuchElementException missingGraphs(List<MissingGraph> missingGraphs, String dbName) {
        if (missingGraphs.size() == 1) {
            return missingGraphs.get(0).exception;
        }

        var message = new StringBuilder("The graphs");
        for (int i = 0; i < missingGraphs.size() - 1; i++) {
            message.append(" `").append(missingGraphs.get(i).name).append("`,");
        }
        message
            .append(" and `")
            .append(missingGraphs.get(missingGraphs.size() - 1).name)
            .append("` do not exist on database `")
            .append(dbName)
            .append("`.");

        var exception = new NoSuchElementException(message.toString());
        for (var missingGraph : missingGraphs) {
            exception.addSuppressed(missingGraph.exception);
        }
        return exception;
    }

    private static final class MissingGraph {
        private final String name;
        private final NoSuchElementException exception;

        private MissingGraph(String name, NoSuchElementException exception) {
            this.name = name;
            this.exception = exception;
        }
    }
}
