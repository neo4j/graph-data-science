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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.FIRST;
import static org.neo4j.procedure.Mode.READ;

@SuppressWarnings("unused")
public final class MemRecProc {

    @Context
    public Log log;

    @Context
    public GraphDatabaseAPI api;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "algo.memrec", mode = READ)
    @Description("Calculates the memory requirements for the given graph and algo. " +
            "CALL algo.memrec(label:String, relationship:String, algo:String, " +
            "{weightProperty:'weight', concurrency:4, ...properties })" +
            "YIELD requiredMemory")
    public Stream<MemRecResult> memrec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "algo", defaultValue = "") String algo,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> configMap) {

        Procedures procedures = api.getDependencyResolver().resolveDependency(Procedures.class, FIRST);
        if (algo != null && !algo.isEmpty()) {
            QualifiedName name = new QualifiedName(Arrays.asList("algo", algo), "memrec");
            ProcedureHandle proc = null;
            try {
                proc = procedures.procedure(name);
            } catch (ProcedureException e) {
                if (!e.status().equals(Status.Procedure.ProcedureNotFound)) {
                    throw ExceptionUtil.asUnchecked(e);
                }
            }
            if (proc != null) {
                String query = " CALL " + proc.signature().name() + "($label, $relationship, $config)";

                Stream.Builder<MemRecResult> builder = Stream.builder();
                api.execute(query, MapUtil.map(
                        "label", label,
                        "relationship", relationship,
                        "config", configMap
                )).accept(row -> {
                    builder.add(new MemRecResult(row));
                    return true;
                });
                return builder.build();
            }
        }

        String available = procedures.getAllProcedures().stream()
                .filter(p -> {
                    String[] namespace = p.name().namespace();
                    return namespace[0].equals("algo")
                           && namespace.length == 2
                           && p.name().name().equals("memrec");
                })
                .map(p -> p.name().namespace())
                .map(ns -> ns[ns.length - 1])
                .distinct()
                .collect(Collectors.joining(", ", "{", "}"));
        final String message;
        if (algo == null || algo.isEmpty()) {
            message = String.format(
                    "Missing procedure parameter, the available and supported procedures are %s.",
                    available);
        } else {
            message = String.format(
                    "The procedure [%s] does not support memrec or does not exist, the available and supported procedures are %s.",
                    algo,
                    available);
        }

        throw new IllegalArgumentException(message);
    }
}
