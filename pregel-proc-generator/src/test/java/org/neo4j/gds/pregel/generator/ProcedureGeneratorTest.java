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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ProcedureGeneratorTest {

    private static final String NL = System.lineSeparator();

    @Test
    void shouldGenerateType() {
        var typeNames = new TypeNames("gds.test", "Bar", ClassName.get("gds.test.config", "BarConfig"));
        var procedureGenerator = new ProcedureGenerator(typeNames, "gds.bar", Optional.empty());
        var typeSpec = procedureGenerator.typeSpec(GDSMode.MUTATE, Optional.empty());
        assertThat(typeSpec.toString()).isEqualTo("" +
            "@org.neo4j.gds.executor.GdsCallable(" + NL +
            "    name = \"gds.bar.mutate\"," + NL +
            "    executionMode = org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY" + NL +
            ")" + NL +
            "public final class BarMutateProc extends org.neo4j.gds.pregel.proc.PregelMutateProc<gds.test.BarAlgorithm, gds.test.config.BarConfig> {" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateProcedureMethod() {
        var typeNames = new TypeNames("gds.test", "Bar", ClassName.get("gds.test.config", "BarConfig"));
        var procedureGenerator = new ProcedureGenerator(typeNames, "gds.bar", Optional.empty());
        var methodSpec = procedureGenerator.procMethod(GDSMode.MUTATE);
        assertThat(methodSpec.toString()).isEqualTo("" +
            "@org.neo4j.procedure.Procedure(" + NL +
            "    name = \"gds.bar.mutate\"," + NL +
            "    mode = org.neo4j.procedure.Mode.READ" + NL +
            ")" + NL +
            "public java.util.stream.Stream<org.neo4j.gds.pregel.proc.PregelMutateResult> mutate(" + NL +
            "    @org.neo4j.procedure.Name(\"graphName\") java.lang.String graphName," + NL +
            "    @org.neo4j.procedure.Name(value = \"configuration\", defaultValue = \"{}\") java.util.Map<java.lang.String, java.lang.Object> configuration) {" + NL +
            "  return mutate(compute(graphName, configuration));" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateEstimateProcedureMethod() {
        var typeNames = new TypeNames("gds.test", "Bar", ClassName.get("gds.test.config", "BarConfig"));
        var procedureGenerator = new ProcedureGenerator(typeNames, "gds.bar", Optional.empty());
        var methodSpec = procedureGenerator.procEstimateMethod(GDSMode.MUTATE);
        assertThat(methodSpec.toString()).isEqualTo("" +
            "@org.neo4j.procedure.Procedure(" + NL +
            "    name = \"gds.bar.mutate.estimate\"," + NL +
            "    mode = org.neo4j.procedure.Mode.READ" + NL +
            ")" + NL +
            "@org.neo4j.procedure.Description(org.neo4j.gds.BaseProc.ESTIMATE_DESCRIPTION)" + NL +
            "public java.util.stream.Stream<org.neo4j.gds.results.MemoryEstimateResult> estimate(" + NL +
            "    @org.neo4j.procedure.Name(\"graphNameOrConfiguration\") java.lang.Object graphNameOrConfiguration," + NL +
            "    @org.neo4j.procedure.Name(\"algoConfiguration\") java.util.Map<java.lang.String, java.lang.Object> algoConfiguration) {" + NL +
            "  return computeEstimate(graphNameOrConfiguration, algoConfiguration);" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateNewConfigMethod() {
        var typeNames = new TypeNames("gds.test", "Bar", ClassName.get("gds.test.config", "BarConfig"));
        var procedureGenerator = new ProcedureGenerator(typeNames, "gds.bar", Optional.empty());
        var methodSpec = procedureGenerator.newConfigMethod();
        assertThat(methodSpec.toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "protected gds.test.config.BarConfig newConfig(java.lang.String username," + NL +
            "    org.neo4j.gds.core.CypherMapWrapper config) {" + NL +
            "  return gds.test.config.BarConfig.of(config);" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateAlgorithmFactoryMethod() {
        var typeNames = new TypeNames("gds.test", "Bar", ClassName.get("gds.test.config", "BarConfig"));
        var procedureGenerator = new ProcedureGenerator(typeNames, "gds.bar", Optional.empty());
        var methodSpec = procedureGenerator.algorithmFactoryMethod();
        assertThat(methodSpec.toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public org.neo4j.gds.GraphAlgorithmFactory<gds.test.BarAlgorithm, gds.test.config.BarConfig> algorithmFactory(" + NL +
            "    org.neo4j.gds.executor.ExecutionContext executionContext) {" + NL +
            "  return new gds.test.BarAlgorithmFactory();" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateValidationConfigMethod() {
        var typeNames = new TypeNames("gds.test", "Bar", ClassName.get("gds.test.config", "BarConfig"));
        var procedureGenerator = new ProcedureGenerator(typeNames, "gds.bar", Optional.empty());
        var methodSpec = procedureGenerator.inverseIndexValidationOverride();
        assertThat(methodSpec.toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public org.neo4j.gds.executor.validation.ValidationConfiguration<gds.test.config.BarConfig> validationConfig(" + NL +
            "    org.neo4j.gds.executor.ExecutionContext executionContext) {" + NL +
            "  return org.neo4j.gds.pregel.proc.PregelBaseProc.ensureIndexValidation(executionContext.log(), executionContext.taskRegistryFactory());" + NL +
            "}" + NL
        );
    }
}
