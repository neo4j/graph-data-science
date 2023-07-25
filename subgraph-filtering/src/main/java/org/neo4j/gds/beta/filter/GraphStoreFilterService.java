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
package org.neo4j.gds.beta.filter;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.filter.expression.SemanticErrors;
import org.neo4j.gds.config.GraphProjectFromGraphConfig;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.concurrent.ExecutorService;

public class GraphStoreFilterService {
    public Task progressTask(GraphStore graphStore) {
        return GraphStoreFilter.progressTask(graphStore);
    }

    /**
     * @throws java.lang.IllegalArgumentException if the filter request was malformed
     */
    public GraphStore filter(
        GraphStore graphStore,
        GraphProjectFromGraphConfig configuration,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        try {
            return GraphStoreFilter.filter(graphStore, configuration, executorService, progressTracker);
        } catch (ParseException | SemanticErrors e) {
            throw new IllegalArgumentException(e);
        }
    }
}
