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
package org.neo4j.gds.pregel.generator;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.pregel.proc.PregelMutateComputationResultConsumer;
import org.neo4j.gds.pregel.proc.PregelMutateProc;
import org.neo4j.gds.pregel.proc.PregelMutateResult;
import org.neo4j.gds.pregel.proc.PregelStatsComputationResultConsumer;
import org.neo4j.gds.pregel.proc.PregelStatsProc;
import org.neo4j.gds.pregel.proc.PregelStatsResult;
import org.neo4j.gds.pregel.proc.PregelStreamComputationResultConsumer;
import org.neo4j.gds.pregel.proc.PregelStreamProc;
import org.neo4j.gds.pregel.proc.PregelStreamResult;
import org.neo4j.gds.pregel.proc.PregelWriteComputationResultConsumer;
import org.neo4j.gds.pregel.proc.PregelWriteProc;
import org.neo4j.gds.pregel.proc.PregelWriteResult;

/**
 * The {@code com.squareup.javapoet.TypeName}s for all types involved in code generation for a Pregel computation.
 */
public class TypeNames {
    private final TypeName config;
    private final ClassName computation;
    private final ClassName algorithm;
    private final ClassName algorithmFactory;

    /**
     * Providing the package and simple class name of the Pregel computation, most other type names are computed.
     * The exception is the procedure configuration, which is a user-provided class with no determinate relation
     * to the computation. The config therefore needs to be provided explicitly.
     *
     * @param packageName the package of the Pregel computation
     * @param computationName the simple class name of the Pregel computation
     * @param config the procedure configuration type
     */
    public TypeNames(String packageName, String computationName, TypeName config) {
        this.computation = ClassName.get(packageName, computationName);
        this.algorithm = this.computation.peerClass(computationName + "Algorithm");
        this.algorithmFactory = this.computation.peerClass(computationName + "AlgorithmFactory");
        this.config = config;
    }

    public ClassName computation() {
        return computation;
    }

    public ClassName algorithm() {
        return algorithm;
    }

    public ClassName algorithmFactory() {
        return algorithmFactory;
    }

    public ClassName procedure(GDSMode mode) {
        var simpleName = computation.simpleName() + mode.camelCase() + "Proc";
        return computation.peerClass(simpleName);
    }

    public ClassName procedureResult(GDSMode mode) {
        return ClassName.get(resultTypeForMode(mode));
    }

    public ClassName procedureBase(GDSMode mode) {
        return ClassName.get(procedureBaseTypeForMode(mode));
    }

    public ClassName specification(GDSMode mode) {
        var simpleName = computation.simpleName() + mode.camelCase() + "Specification";
        return computation.peerClass(simpleName);
    }

    public ClassName computationResultConsumer(GDSMode mode) {
        return ClassName.get(computationResultConsumerTypeForMode(mode));
    }

    public TypeName config() {
        return config;
    }

    private Class<?> resultTypeForMode(GDSMode mode) {
        switch (mode) {
            case STATS: return PregelStatsResult.class;
            case WRITE: return PregelWriteResult.class;
            case MUTATE: return PregelMutateResult.class;
            case STREAM: return PregelStreamResult.class;
            default: throw new IllegalStateException("Unexpected value: " + mode);
        }
    }

    private Class<?> computationResultConsumerTypeForMode(GDSMode mode) {
        switch (mode) {
            case STATS: return PregelStatsComputationResultConsumer.class;
            case WRITE: return PregelWriteComputationResultConsumer.class;
            case MUTATE: return PregelMutateComputationResultConsumer.class;
            case STREAM: return PregelStreamComputationResultConsumer.class;
            default: throw new IllegalStateException("Unexpected value: " + mode);
        }
    }

    private Class<?> procedureBaseTypeForMode(GDSMode mode) {
        switch (mode) {
            case STATS: return PregelStatsProc.class;
            case WRITE: return PregelWriteProc.class;
            case MUTATE: return PregelMutateProc.class;
            case STREAM: return PregelStreamProc.class;
            default: throw new IllegalStateException("Unexpected value: " + mode);
        }
    }
}
