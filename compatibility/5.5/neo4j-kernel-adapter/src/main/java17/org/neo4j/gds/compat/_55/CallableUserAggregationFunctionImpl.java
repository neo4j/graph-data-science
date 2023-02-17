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
package org.neo4j.gds.compat._55;

import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.compat.CompatUserAggregationFunction;
import org.neo4j.gds.compat.CompatUserAggregator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserAggregationUpdater;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.values.AnyValue;

@SuppressForbidden(reason = "This is the compat API")
public final class CallableUserAggregationFunctionImpl implements CallableUserAggregationFunction {
    private final CompatUserAggregationFunction function;

    CallableUserAggregationFunctionImpl(CompatUserAggregationFunction function) {
        this.function = function;
    }

    @Override
    public UserFunctionSignature signature() {
        return this.function.signature();
    }

    @Override
    public UserAggregationReducer createReducer(Context ctx) throws ProcedureException {
        return new UserAggregatorImpl(this.function.create(ctx));
    }

    private static final class UserAggregatorImpl implements UserAggregationReducer, UserAggregationUpdater {
        private final CompatUserAggregator aggregator;

        private UserAggregatorImpl(CompatUserAggregator aggregator) {
            this.aggregator = aggregator;
        }

        @Override
        public UserAggregationUpdater newUpdater() {
            return this;
        }

        @Override
        public void update(AnyValue[] input) throws ProcedureException {
            this.aggregator.update(input);
        }

        @Override
        public void applyUpdates() {
        }

        @Override
        public AnyValue result() throws ProcedureException {
            return this.aggregator.result();
        }
    }
}
