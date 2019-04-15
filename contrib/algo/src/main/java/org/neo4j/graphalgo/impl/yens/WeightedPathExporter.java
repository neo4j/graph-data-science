/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl.yens;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Specialized exporter for {@link WeightedPath}
 *
 * @author mknblch
 */
public class WeightedPathExporter extends StatementApi {

    private final IdMapping idMapping;
    private final RelationshipWeights relationshipWeights;
    private final String relPrefix;
    private final ExecutorService executorService;
    private final String propertyName;

    public WeightedPathExporter(GraphDatabaseAPI api,
                                ExecutorService executorService,
                                IdMapping idMapping,
                                RelationshipWeights relationshipWeights,
                                String relPrefix,
                                String propertyName) {
        super(api);
        this.executorService = executorService;
        this.idMapping = idMapping;
        this.relationshipWeights = relationshipWeights;
        this.relPrefix = relPrefix;
        this.propertyName = propertyName;
    }

    /**
     * export a list of weighted paths
     * @param paths
     */
    public void export(List<WeightedPath> paths) {
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(paths);
        } else {
            writeSequential(paths);
        }
    }

    private void export(String relationshipType, String propertyName, WeightedPath path) {
        applyInTransaction(statement -> {
            final int relId = statement.tokenWrite().relationshipTypeGetOrCreateForName(relationshipType);
            if (relId == -1) {
                throw new IllegalStateException("no write property id is set");
            }
            path.forEachEdge((s, t) -> {
                try {
                    long relationshipId = statement.dataWrite().relationshipCreate(
                            idMapping.toOriginalNodeId(s),
                            relId,
                            idMapping.toOriginalNodeId(t)
                    );

                    statement.dataWrite().relationshipSetProperty(
                            relationshipId,
                            getOrCreatePropertyId(propertyName),
                            Values.doubleValue(relationshipWeights.weightOf(s, t)));
                } catch (KernelException e) {
                    ExceptionUtil.throwKernelException(e);
                }
            });
            return null;
        });

    }

    private int getOrCreatePropertyId(String propertyName) {
        return applyInTransaction(stmt -> stmt
                .tokenWrite()
                .propertyKeyGetOrCreateForName(propertyName));
    }

    private void writeSequential(List<WeightedPath> paths) {
        final Pointer.IntPointer counter = Pointer.wrap(0);
        paths.stream()
                .sorted(WeightedPath.comparator())
                .forEach(path ->
                        export(String.format("%s%d", relPrefix, counter.v++), propertyName, path));
    }

    private void writeParallel(List<WeightedPath> paths) {
        final Pointer.IntPointer counter = Pointer.wrap(0);

        Stream<Pair<WeightedPath, String>> pathsAndRelTypes = paths.stream().sorted(WeightedPath.comparator())
                .map(path -> Pair.of(path, String.format("%s%d", relPrefix, counter.v++)));

        final List<Runnable> tasks = pathsAndRelTypes
                .map(pair -> (Runnable) () ->  export(pair.other(), propertyName, pair.first()))
                .collect(Collectors.toList());
        ParallelUtil.run(tasks, executorService);
    }

}
