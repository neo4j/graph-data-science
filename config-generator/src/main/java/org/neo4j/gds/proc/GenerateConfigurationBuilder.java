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
package org.neo4j.gds.proc;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.core.CypherMapWrapper;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.auto.common.MoreTypes.isTypeOf;
import static org.neo4j.gds.proc.TypeUtil.unwrapOptionalType;

class GenerateConfigurationBuilder {
    private final Messager messager;

    GenerateConfigurationBuilder(Messager messager) {this.messager = messager;}

    TypeSpec defineConfigBuilder(
        ConfigParser.Spec config,
        String packageName,
        String generatedClassName,
        List<ParameterSpec> constructorParameters,
        String configMapParameterName,
        Optional<MethodSpec> maybeFactoryFunction
    ) {
        ClassName builderClassName = ClassName.get(packageName, generatedClassName + ".Builder");
        TypeSpec.Builder configBuilderClass = TypeSpec.classBuilder("Builder")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL);

        // add config map to handle config keys
        configBuilderClass.addField(
            ParameterizedTypeName.get(Map.class, String.class, Object.class),
            configMapParameterName,
            Modifier.FINAL,
            Modifier.PRIVATE
        );
        configBuilderClass.addMethod(MethodSpec
            .constructorBuilder()
            .addStatement("this.$N = new $T<>()", configMapParameterName, HashMap.class)
            .addModifiers(Modifier.PUBLIC).build());


        // Config Key -> put into a map
        // config parameter -> set field on builder

        // add parameter fields to builder
        constructorParameters
            .stream()
            .filter(p -> !p.name.equals(configMapParameterName))
            .forEach(parameter -> configBuilderClass.addField(
                parameter.type,
                parameter.name,
                Modifier.PRIVATE
            ));

        // FIXME correct handling of ConvertWith function (use type of convertWith on the builder function instead !

        String constructorParameterString = constructorParameters
            .stream()
            .map(param -> param.name)
            .collect(Collectors.joining(", "));

        var configCreateStatement = maybeFactoryFunction
            .map(factoryFunc -> CodeBlock.of("return $L.$L($L)", generatedClassName, factoryFunc.name, constructorParameterString))
            .orElse(CodeBlock.of("return new $L($L)", generatedClassName, constructorParameterString));

        MethodSpec buildMethod = MethodSpec.methodBuilder("build")
            .addModifiers(Modifier.PUBLIC)
            .addCode(CodeBlock.builder()
                .addStatement(
                    "$1T $2N = $1T.create(this.$3N)",
                    CypherMapWrapper.class,
                    configMapParameterName,
                    configMapParameterName
                ).addStatement(configCreateStatement).build())
            .returns(TypeName.get(config.rootType())).build();
        
        return configBuilderClass
            .addMethods(defineBuilderSetters(config.members(), configMapParameterName, builderClassName))
            .addMethod(buildMethod)
            .build();
    }

    private List<MethodSpec> defineBuilderSetters(
        List<ConfigParser.Member> members,
        String builderConfigMapFieldName,
        ClassName builderClassName
    ) {
        return members.stream()
            .filter(ConfigParser.Member::isConfigValue)
            .flatMap(member -> {
                var setterMethods = Stream.<MethodSpec>builder();

                MethodSpec.Builder setMethodBuilder = MethodSpec.methodBuilder(member.methodName())
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(builderMemberType(member, member.isConfigMapEntry()), member.methodName())
                    .returns(builderClassName);

                CodeBlock.Builder setterCode = CodeBlock.builder();

                if (member.isConfigMapEntry()) {
                    setterCode.addStatement(
                        "this.$N.put(\"$L\", $N)",
                        builderConfigMapFieldName,
                        member.lookupKey(),
                        member.methodName()
                    );
                } else {
                    setterCode.addStatement("this.$1N = $1N", member.methodName());
                }

                setMethodBuilder.addCode(setterCode.build());
                setterMethods.add(setMethodBuilder.addStatement("return this").build());

                // for map entries we provide both; for positional arguments only the raw optional version
                if (member.isConfigMapEntry() && isTypeOf(Optional.class, member.method().getReturnType())) {
                    var optionalSetterBuilder = MethodSpec.methodBuilder(member.methodName())
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(TypeName.get(member.method().getReturnType()), member.methodName())
                        .returns(builderClassName);

                    String lambdaVarName = "actual" + member.methodName();
                    if (member.isConfigMapEntry()) {
                        optionalSetterBuilder.addStatement(
                            "$1N.ifPresent($2N -> this.$3N.put(\"$4L\", $2N))",
                            member.methodName(),
                            lambdaVarName,
                            builderConfigMapFieldName,
                            member.lookupKey()
                        );
                    } else {
                        optionalSetterBuilder.addStatement("$1N.ifPresent($2N ->this.$1N = $2N)", member.methodName(), lambdaVarName);
                    }

                    setterMethods.add(optionalSetterBuilder.addStatement("return this").build());
                }

                return setterMethods.build();
            })
            .collect(Collectors.toList());
    }

    private TypeName builderMemberType(ConfigParser.Member member, boolean isConfigMapEntry) {
        TypeMirror returnType = member.method().getReturnType();

        // for config parameters we don't want to unwrap the Optional
        if (isConfigMapEntry && isTypeOf(Optional.class, returnType)) {
            Optional<ClassName> maybeType = unwrapOptionalType(member, (DeclaredType) returnType, messager);
            if (maybeType.isPresent()) {
                return maybeType.get();
            }
        }

        return TypeName.get(returnType);
    }
}
