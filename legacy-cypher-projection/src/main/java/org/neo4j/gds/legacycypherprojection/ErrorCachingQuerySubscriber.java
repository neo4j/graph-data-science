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
package org.neo4j.gds.legacycypherprojection;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

import java.util.Optional;

public abstract class ErrorCachingQuerySubscriber implements QuerySubscriber {

    @Nullable
    private RuntimeException error;

    public Optional<RuntimeException> error() {
        return Optional.ofNullable(error);
    }

    public void onError(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            this.error = (RuntimeException) throwable;
        } else if (throwable instanceof QueryExecutionKernelException) {
            this.error = ((QueryExecutionKernelException) throwable).asUserException();
        } else {
            this.error = new RuntimeException(throwable);
        }
    }

    public static final class DoNothingSubscriber extends ErrorCachingQuerySubscriber {
        @Override
        public void onResult(int numberOfFields) {
        }

        @Override
        public void onRecord() {
        }

        @Override
        public void onField(int offset, AnyValue value) {
        }

        @Override
        public void onRecordCompleted() {
        }

        @Override
        public void onResultCompleted(QueryStatistics statistics) {
        }
    }
}
