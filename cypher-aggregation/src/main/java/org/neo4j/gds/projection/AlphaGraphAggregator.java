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

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

// public is required for the Cypher runtime
@SuppressWarnings("WeakerAccess")
public class AlphaGraphAggregator extends GraphAggregator {

    AlphaGraphAggregator(
        DatabaseId databaseId,
        String username,
        WriteMode writeMode
    ) {
        super(databaseId, username, writeMode);
    }

    @Override
    public void update(AnyValue[] input) throws ProcedureException {
        try {
            super.projectNextRelationship(
                (TextValue) input[0],
                input[1],
                input[2],
                input[3],
                input[4],
                input[5]
            );
        } catch (Exception e) {
            throw new ProcedureException(
                Status.Procedure.ProcedureCallFailed,
                e,
                "Failed to invoke function `%s`: Caused by: %s",
                AlphaCypherAggregation.FUNCTION_NAME,
                e
            );
        }
    }
}
