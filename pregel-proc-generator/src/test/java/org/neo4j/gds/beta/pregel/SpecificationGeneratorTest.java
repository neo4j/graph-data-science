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
import org.neo4j.gds.beta.pregel.annotation.GDSMode;

import static org.assertj.core.api.Assertions.assertThat;

class SpecificationGeneratorTest {

    private static final String NL = System.lineSeparator();

    @Test
    void shouldGenerateType() {
        var specificationGenerator = new SpecificationGenerator("gds.test", "Foo");
        var configTypeName = TypeName.get(PregelConfig.class);
        var specificationType = specificationGenerator.typeSpec(configTypeName, GDSMode.STATS).build();

        assertThat(specificationType.toString()).isEqualTo("" +
            "public final class FooAlgorithmSpecification extends org.neo4j.gds.executor.AlgorithmSpec<" +
            "gds.test.FooAlgorithm, " +
            "org.neo4j.gds.beta.pregel.PregelResult, " +
            "org.neo4j.gds.beta.pregel.PregelConfig, " +
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
        var configTypeName = TypeName.get(PregelConfig.class);
        assertThat(specificationGenerator.newConfigFunctionMethod(configTypeName).toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public org.neo4j.gds.executor.NewConfigFunction<org.neo4j.gds.beta.pregel.PregelConfig> newConfigFunction(" + NL +
            "    ) {" + NL +
            "  return (__, userInput) -> org.neo4j.gds.beta.pregel.PregelConfig.of(userInput);" + NL +
            "}" + NL
        );
    }
}
