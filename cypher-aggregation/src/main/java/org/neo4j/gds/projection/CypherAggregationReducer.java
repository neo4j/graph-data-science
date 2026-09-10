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
package org.neo4j.gds.projection;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.gds.projection.CypherAggregationUpdater.InputValuesMapper;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserAggregationUpdater;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.neo4j.gds.projection.CypherAggregation.FUNCTION_NAME;

public class CypherAggregationReducer implements UserAggregationReducer, AutoCloseable {

    private final AtomicBoolean completedSuccessfully;

    private final LazyGraphImporter lazyGraphImporter;
    private final ProjectionMetricsService projectionMetricsService;

    private final DatabaseId databaseId;
    private final ProgressTimer progressTimer;
    private final ExtractNodeId extractNodeId;
    private final InputValuesMapper inputValuesMapper;

    private final Collection<CypherAggregationUpdater> updaters;

    // #result() may be called twice, we cache the result of the first call to return it again in the second invocation
    private @Nullable ProjectionResult result;

    public CypherAggregationReducer(
        LazyGraphImporter lazyGraphImporter,
        ProjectionMetricsService projectionMetricsService,
        DatabaseId databaseId,
        ExtractNodeId extractNodeId,
        InputValuesMapper inputValuesMapper
    ) {
        this.lazyGraphImporter = lazyGraphImporter;
        this.projectionMetricsService = projectionMetricsService;
        this.databaseId = databaseId;
        this.extractNodeId = extractNodeId;
        this.inputValuesMapper = inputValuesMapper;
        this.updaters = new ArrayList<>();
        completedSuccessfully = new AtomicBoolean(false);
        progressTimer = ProgressTimer.start();
    }

    @Override
    public UserAggregationUpdater newUpdater() throws ProcedureException {
        var updater = new CypherAggregationUpdater(lazyGraphImporter, extractNodeId, inputValuesMapper);
        updaters.add(updater);
        return updater;
    }

    @Override
    public AnyValue result() throws ProcedureException {
        try {
            var projectionMetric = projectionMetricsService.createCypherV2();
            ProjectionResult result;
            try (projectionMetric) {
                projectionMetric.start();
                result = buildGraph();
            } catch (Exception e) {
                projectionMetric.failed(e);
                throw e;
            }

            if (result == null) {
                return Values.NO_VALUE;
            }

            var builder = new MapValueBuilder(6);
            builder.add("graphName", Values.stringValue(result.graphName()));
            builder.add("nodeCount", Values.longValue(result.nodeCount()));
            builder.add("relationshipCount", Values.longValue(result.relationshipCount()));
            builder.add("projectMillis", Values.longValue(result.projectMillis()));
            builder.add("configuration", ValueUtils.asAnyValue(result.configuration()));
            builder.add("query", ValueUtils.asAnyValue(result.query()));
            MapValue projectResult = builder.build();

            this.completedSuccessfully.set(true);

            return projectResult;
        } catch (Throwable T) {
            throw ProcedureException.invocationFailed("function", FUNCTION_NAME.toString(), T);
        }
    }

    @Override
    public void close() throws Exception {
        if (!completedSuccessfully.get()) {
            updaters.forEach(CypherAggregationUpdater::close);
        }
    }

    public @Nullable ProjectionResult buildGraph() {
        var importer = lazyGraphImporter.getImporter();
        if (importer == null) {
            // Nothing aggregated
            return null;
        }

        // Older cypher runtimes call the result method multiple times, we cache the result of the first call
        if (this.result != null) {
            return this.result;
        }

        var databaseInfo = DatabaseInfo.create(
            this.databaseId,
            DatabaseInfo.DatabaseLocation.LOCAL
        );

        this.result = importer.result(
            databaseInfo,
            this.progressTimer,
            extractNodeId.hasSeenArbitraryIds()
        );

        return this.result;
    }

}
