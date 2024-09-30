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
package org.neo4j.gds.compat._525;

import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.compat.CompatCallableProcedure;
import org.neo4j.gds.compat.Neo4jProxyApi;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;

import static org.neo4j.internal.helpers.collection.Iterators.asRawIterator;

public final class Neo4jProxyImpl implements Neo4jProxyApi {

    @Override
    public void rethrowUnlessDuplicateRegistration(ProcedureException e) throws KernelException {
        if (e.status() == Status.Procedure.ProcedureRegistrationFailed && e.getMessage().contains("already in use")) {
            return;
        }
        throw e;
    }

    @Override
    public CallableProcedure callableProcedure(CompatCallableProcedure procedure) {
        @SuppressForbidden(reason = "This is the compat API")
        final class CallableProcedureImpl implements CallableProcedure {
            private final CompatCallableProcedure procedure;

            private CallableProcedureImpl(CompatCallableProcedure procedure) {
                this.procedure = procedure;
            }

            @Override
            public ProcedureSignature signature() {
                return this.procedure.signature();
            }

            @Override
            public ResourceRawIterator<AnyValue[], ProcedureException> apply(
                Context context,
                AnyValue[] anyValues,
                ResourceMonitor resourceMonitor
            ) throws ProcedureException {
                return asRawIterator(this.procedure.apply(context, anyValues));
            }
        }

        return new CallableProcedureImpl(procedure);
    }

    @Override
    public int sequenceSizeAsInt(SequenceValue sequenceValue) {
        return sequenceValue.intSize();
    }

    @Override
    public AnyValue sequenceValueAt(SequenceValue sequenceValue, int index) {
        return sequenceValue.value((long) index);
    }

    @Override
    public RuntimeException queryExceptionAsRuntimeException(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            return (RuntimeException) throwable;
        } else if (throwable instanceof QueryExecutionKernelException) {
            return ((QueryExecutionKernelException) throwable).asUserException();
        } else {
            return new RuntimeException(throwable);
        }
    }

    @Override
    public ProcedureException procedureCallFailed(String message, Object... args) {
        return new ProcedureException(Status.Procedure.ProcedureCallFailed, message, args);
    }

    @Override
    public ProcedureException procedureCallFailed(Throwable reason, String message, Object... args) {
        return new ProcedureException(Status.Procedure.ProcedureCallFailed, reason, message, args);
    }

    @Override
    public String exceptionMessage(Throwable e) {
        return e.getMessage();
    }

    @Override
    public DatabaseNotFoundException databaseNotFoundException(String message) {
        throw new DatabaseNotFoundException(message);
    }
}
