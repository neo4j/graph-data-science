/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

class CountingCypherRecordLoader extends CypherRecordLoader<BatchLoadResult> {

    private long total;

    CountingCypherRecordLoader(String cypherQuery, GraphDatabaseAPI api, GraphSetup setup) {
        super(cypherQuery, NO_COUNT, api, setup);
    }

    @Override
    BatchLoadResult loadOneBatch(long offset, int batchSize, int bufferSize) {
        ResultCountingVisitor visitor = new ResultCountingVisitor();
        runLoadingQuery(offset, batchSize, visitor);
        return new BatchLoadResult(offset, visitor.rows(), -1L, -1L);
    }

    @Override
    void updateCounts(BatchLoadResult result) {
        total += result.rows();
    }

    @Override
    BatchLoadResult result() {
        return new BatchLoadResult(0, total, -1L, -1L);
    }
}
