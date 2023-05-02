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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.test.TestProc;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Neo4jModelCatalogExtension
class ProcedureRunnerTest extends BaseTest {

    @Inject
    ModelCatalog modelCatalog;

    @Test
    void shouldValidateThatAllContextFieldsAreSet() {
        var instantiateProcedureMethodName = "instantiateProcedure";
        var instantiateProcedureMethod = Arrays.stream(ProcedureRunner.class.getMethods())
            .filter(method -> method.getName().equals(instantiateProcedureMethodName))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(formatWithLocale(
                "Did not find any method with name",
                instantiateProcedureMethodName
            )));

        var baseProcClass = BaseProc.class;
        List<Class<?>> contextFieldTypes = Arrays.stream(baseProcClass.getDeclaredFields())
            .filter(field -> field.isAnnotationPresent(Context.class))
            .map(Field::getType)
            // KernelTransaction is computed from Transaction
            // ModelCatalog is resolved from GraphDatabaseAPI
            .filter(paramType -> paramType != KernelTransaction.class && paramType != ModelCatalog.class)
            .collect(Collectors.toList());

        var parameterTypes = Arrays.asList(instantiateProcedureMethod.getParameterTypes());

        contextFieldTypes.removeAll(parameterTypes);
        assertThat(contextFieldTypes)
            .overridingErrorMessage(
                formatWithLocale(
                    "Expecting method %s to set all context injected fields on %s but did not find parameters for types %s",
                    instantiateProcedureMethodName,
                    baseProcClass.getSimpleName(),
                    contextFieldTypes
                )
            )
            .isEmpty();
    }

    @Test
    void shouldPassCorrectParameters() {
        try (var tx = db.beginTx()) {
            var procedureCallContext = ProcedureCallContext.EMPTY;
            var log = Neo4jProxy.testLog();
            var username = Username.of("foo");
            TaskRegistryFactory taskRegistryFactory = jobId -> new TaskRegistry(
                username.username(),
                new GlobalTaskStore(),
                jobId
            );
            ProcedureRunner.applyOnProcedure(
                db,
                TestProc.class,
                procedureCallContext,
                log,
                taskRegistryFactory,
                tx,
                username,
                proc -> {
                    assertThat(proc.procedureTransaction).isEqualTo(tx);
                    assertThat(proc.callContext).isEqualTo(procedureCallContext);
                    assertThat(proc.log).isEqualTo(log);
                    assertThat(proc.taskRegistryFactory).isEqualTo(taskRegistryFactory);
                    assertThat(proc.username).isEqualTo(username);
                    assertThat(proc.executionContext().modelCatalog()).isEqualTo(modelCatalog);
                }
            );
        }
    }
}
