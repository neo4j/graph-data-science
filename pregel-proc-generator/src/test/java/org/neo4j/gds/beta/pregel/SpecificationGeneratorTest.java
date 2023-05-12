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
package org.neo4j.gds.beta.pregel;

import com.squareup.javapoet.TypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;

import static org.assertj.core.api.Assertions.assertThat;

class SpecificationGeneratorTest {

    private static final String NL = System.lineSeparator();

    @Test
    void shouldGenerateType() {
        var specificationGenerator = new SpecificationGenerator("gds.test", "Foo");
        var configTypeName = TypeName.get(PregelProcedureConfig.class);
        var specificationType = specificationGenerator.typeSpec(configTypeName, GDSMode.STATS).build();

        assertThat(specificationType.toString()).isEqualTo("" +
            "public final class FooAlgorithmSpecification extends org.neo4j.gds.executor.AlgorithmSpec<" +
            "gds.test.FooAlgorithm, " +
            "org.neo4j.gds.beta.pregel.PregelResult, " +
            "org.neo4j.gds.beta.pregel.PregelProcedureConfig, " +
            "org.neo4j.gds.pregel.proc.PregelStatsResult, " +
            "gds.test.FooAlgorithmFactory> {" +
            System.lineSeparator() +
            "}" +
            System.lineSeparator()
        );
    }

    @Test
    void shouldGenerateNameMethod() {
        var specificationGenerator = new SpecificationGenerator("gds.test", "Foo");
        assertThat(specificationGenerator.nameMethod().toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public java.lang.String name() {" + NL +
            "  return gds.test.FooAlgorithm.class.getSimpleName();" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateAlgorithmFactoryMethod() {
        var specificationGenerator = new SpecificationGenerator("gds.test", "Foo");
        assertThat(specificationGenerator.algorithmFactoryMethod().toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public gds.test.FooAlgorithmFactory algorithmFactory(" + NL +
            "    org.neo4j.gds.executor.ExecutionContext executionContext) {" + NL +
            "  return new gds.test.FooAlgorithmFactory();" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateNewConfigFunctionMethod() {
        var specificationGenerator = new SpecificationGenerator("gds.test", "Foo");
        var configTypeName = TypeName.get(PregelProcedureConfig.class);
        assertThat(specificationGenerator.newConfigFunctionMethod(configTypeName).toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public org.neo4j.gds.executor.NewConfigFunction<org.neo4j.gds.beta.pregel.PregelProcedureConfig> newConfigFunction(" + NL +
            "    ) {" + NL +
            "  return (__, userInput) -> org.neo4j.gds.beta.pregel.PregelProcedureConfig.of(userInput);" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateComputationResultConsumerMethod() {
        var specificationGenerator = new SpecificationGenerator("gds.test", "Foo");
        var configTypeName = TypeName.get(PregelProcedureConfig.class);
        var computationResultConsumerMethod = specificationGenerator.computationResultConsumerMethod(configTypeName, GDSMode.STATS).toString();
        assertThat(computationResultConsumerMethod).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public org.neo4j.gds.executor.ComputationResultConsumer<gds.test.FooAlgorithm, org.neo4j.gds.beta.pregel.PregelProcedureConfig> computationResultConsumer(" + NL +
            "    ) {" + NL +
            "  return new org.neo4j.gds.pregel.proc.PregelStatsComputationResultConsumer<>();" + NL +
            "}" + NL
        );
    }

    @EnumSource(value = GDSMode.class, names = {"STATS", "STREAM", "MUTATE"})
    @ParameterizedTest
    void shouldGenerateDifferentlyPerMode(GDSMode mode) {
        var specificationGenerator = new SpecificationGenerator("gds.test", "Foo");
        var configTypeName = TypeName.get(PregelProcedureConfig.class);
        var specificationType = specificationGenerator.typeSpec(configTypeName, mode).build();
        assertThat(specificationType.toString())
            .contains("org.neo4j.gds.pregel.proc.Pregel" + mode.camelCase() + "Result");
        var computationResultConsumerMethod = specificationGenerator.computationResultConsumerMethod(configTypeName, mode);
        assertThat(computationResultConsumerMethod.toString())
            .contains("org.neo4j.gds.pregel.proc.Pregel" + mode.camelCase() + "ComputationResultConsumer");
    }
}
