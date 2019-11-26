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

package org.neo4j.graphalgo.proc;

import com.google.auto.common.GeneratedAnnotationSpecs;
import com.google.common.collect.ImmutableList;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.NameAllocator;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.Configuration.ConvertWith;
import org.neo4j.graphalgo.annotation.Configuration.Parameter;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.processing.Messager;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.auto.common.MoreElements.asType;
import static com.google.auto.common.MoreElements.getAnnotationMirror;
import static com.google.auto.common.MoreTypes.asTypeElement;
import static com.google.auto.common.MoreTypes.isTypeOf;
import static javax.lang.model.util.ElementFilter.methodsIn;

final class GenerateConfiguration {

    private static final String CONFIG_VAR = "config";
    private static final AnnotationSpec NULLABLE = AnnotationSpec.builder(Nullable.class).build();
    private static final AnnotationSpec NOT_NULL = AnnotationSpec.builder(NotNull.class).build();

    private final Messager messager;
    private final Elements elementUtils;
    private final Types typeUtils;
    private final SourceVersion sourceVersion;

    GenerateConfiguration(Messager messager, Elements elementUtils, Types typeUtils, SourceVersion sourceVersion) {
        this.messager = messager;
        this.elementUtils = elementUtils;
        this.typeUtils = typeUtils;
        this.sourceVersion = sourceVersion;
    }

    JavaFile generateConfig(ConfigParser.Spec config, String className) {
        PackageElement rootPackage = elementUtils.getPackageOf(config.root());
        String packageName = rootPackage.getQualifiedName().toString();
        TypeSpec typeSpec = process(config, packageName, className);
        return JavaFile.builder(packageName, typeSpec).skipJavaLangImports(true).build();
    }

    private TypeSpec process(ConfigParser.Spec config, String packageName, String generatedClassName) {
        FieldDefinitions fieldDefinitions = defineFields(config);
        return classBuilder(config, packageName, generatedClassName)
            .addFields(fieldDefinitions.fields())
            .addMethods(defineConstructors(config, fieldDefinitions.names()))
            .addMethods(defineGetters(config, fieldDefinitions.names()))
            .build();
    }

    private TypeSpec.Builder classBuilder(ConfigParser.Spec config, String packageName, String generatedClassName) {
        TypeSpec.Builder classBuilder = createNewClass(config, packageName, generatedClassName);
        inheritFrom(classBuilder, config);
        addGeneratedAnnotation(classBuilder);
        return classBuilder;
    }

    private TypeSpec.Builder createNewClass(ConfigParser.Spec config, String packageName, String generatedClassName) {
        return TypeSpec
            .classBuilder(ClassName.get(packageName, generatedClassName))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addOriginatingElement(config.root());
    }

    private void inheritFrom(TypeSpec.Builder classBuilder, ConfigParser.Spec config) {
        classBuilder.addSuperinterface(TypeName.get(config.rootType()));
    }

    private void addGeneratedAnnotation(TypeSpec.Builder classBuilder) {
        GeneratedAnnotationSpecs.generatedAnnotationSpec(
            elementUtils,
            sourceVersion,
            ConfigurationProcessor.class
        ).ifPresent(classBuilder::addAnnotation);
    }

    private FieldDefinitions defineFields(ConfigParser.Spec config) {
        NameAllocator names = new NameAllocator();
        ImmutableFieldDefinitions.Builder builder = ImmutableFieldDefinitions.builder().names(names);
        config.members().stream().map(member ->
            FieldSpec.builder(
                TypeName.get(member.method().getReturnType()),
                names.newName(member.methodName(), member),
                Modifier.PRIVATE, Modifier.FINAL
            ).build()
        ).forEach(builder::addField);
        return builder.build();
    }

    private Iterable<MethodSpec> defineConstructors(ConfigParser.Spec config, NameAllocator names) {
        MethodSpec.Builder configMapConstructor = MethodSpec
            .constructorBuilder()
            .addModifiers(Modifier.PUBLIC);

        MethodSpec.Builder allParametersConstructor = MethodSpec
            .constructorBuilder()
            .addModifiers(Modifier.PUBLIC);

        String configParamterName = names.newName(CONFIG_VAR, CONFIG_VAR);
        boolean requiredMapParameter = false;

        for (ConfigParser.Member member : config.members()) {
            Optional<MemberDefinition> memberDefinition = memberDefinition(names, member);
            if (memberDefinition.isPresent()) {
                ExecutableElement method = member.method();
                MemberDefinition definition = memberDefinition.get();

                Parameter parameter = method.getAnnotation(Parameter.class);
                if (parameter == null) {
                    requiredMapParameter = true;
                    addConfigGetterToConstructor(
                        configMapConstructor,
                        definition
                    );
                } else {
                    addParameterToPrimaryConstructor(
                        configMapConstructor,
                        definition,
                        parameter
                    );
                }

                addParameterToSecondaryConstructor(
                    allParametersConstructor,
                    definition
                );
            }
        }

        if (requiredMapParameter) {
            configMapConstructor.addParameter(
                TypeName.get(CypherMapWrapper.class).annotated(NOT_NULL),
                configParamterName
            );
        }

        MethodSpec primaryConstructor = configMapConstructor.build();
        MethodSpec secondaryConstructor = allParametersConstructor.build();

        List<TypeName> primmaryParameters = primaryConstructor.parameters
            .stream()
            .map(p -> p.type.withoutAnnotations())
            .collect(Collectors.toList());
        List<TypeName> secondaryParamters = secondaryConstructor.parameters
            .stream()
            .map(p -> p.type.withoutAnnotations())
            .collect(Collectors.toList());
        boolean identicalSignature = primmaryParameters.equals(secondaryParamters);

        if (identicalSignature) {
            return ImmutableList.of(primaryConstructor);
        } else {
            return ImmutableList.of(primaryConstructor, secondaryConstructor);
        }
    }

    private void addConfigGetterToConstructor(
        MethodSpec.Builder constructor,
        MemberDefinition definition
    ) {
        CodeBlock.Builder code = CodeBlock.builder().add(
            "$N.$L$L($S",
            definition.configParamName(),
            definition.methodPrefix(),
            definition.methodName(),
            definition.configKey()
        );
        definition.defaultProvider().ifPresent(d -> code.add(", $L", d));
        definition.expectedType().ifPresent(t -> code.add(", $L", t));
        CodeBlock codeBlock = code.add(")").build();
        for (UnaryOperator<CodeBlock> converter : definition.converters()) {
            codeBlock = converter.apply(codeBlock);
        }

        constructor.addStatement("this.$N = $L", definition.fieldName(), codeBlock);
    }

    private void addParameterToPrimaryConstructor(
        MethodSpec.Builder constructor,
        MemberDefinition definition,
        Parameter parameter
    ) {
        TypeName paramType = TypeName.get(definition.parameterType());

        CodeBlock valueProducer;
        if (definition.parameterType().getKind() == TypeKind.DECLARED) {
            if (parameter.acceptNull()) {
                paramType = paramType.annotated(NULLABLE);
                valueProducer = CodeBlock.of("$N", definition.fieldName());
            } else {
                paramType = paramType.annotated(NOT_NULL);
                valueProducer = CodeBlock.of(
                    "$T.requireValue($S, $N)",
                    CypherMapWrapper.class,
                    definition.configKey(),
                    definition.fieldName()
                );
            }
        } else {
            valueProducer = CodeBlock.of("$N", definition.fieldName());
        }

        for (UnaryOperator<CodeBlock> converter : definition.converters()) {
            valueProducer = converter.apply(valueProducer);
        }
        constructor
            .addParameter(paramType, definition.fieldName())
            .addStatement("this.$N = $L", definition.fieldName(), valueProducer);
    }

    private void addParameterToSecondaryConstructor(
        MethodSpec.Builder constructor,
        MemberDefinition definition
    ) {
        TypeName paramType = TypeName.get(definition.fieldType());

        CodeBlock valueProducer;
        if (definition.fieldType().getKind() == TypeKind.DECLARED) {
            paramType = paramType.annotated(NOT_NULL);
            valueProducer = CodeBlock.of(
                "this.$3N = $1T.requireValue($2S, $3N)",
                CypherMapWrapper.class,
                definition.configKey(),
                definition.fieldName()
            );
        } else {
            valueProducer = CodeBlock.of("this.$1N = $1N", definition.fieldName());
        }

        constructor
            .addParameter(paramType, definition.fieldName())
            .addStatement(valueProducer);
    }

    private Optional<MemberDefinition> memberDefinition(NameAllocator names, ConfigParser.Member member) {
        ExecutableElement method = member.method();
        TypeMirror targetType = method.getReturnType();
        ConvertWith convertWith = method.getAnnotation(ConvertWith.class);
        if (convertWith == null) {
            return memberDefinition(names, member, targetType);
        }

        String converter = convertWith.value().trim();
        if (converter.isEmpty()) {
            return converterError(method, "Empty conversion method is not allowed");
        }

        if (!converter.contains("#")) {
            return memberDefinition(
                names,
                member,
                targetType,
                asType(method.getEnclosingElement()),
                converter,
                true
            );
        }

        String[] nameParts = converter.split(Pattern.quote("#"), 2);
        String methodName = nameParts[1];
        if (methodName.isEmpty() || methodName.contains("#")) {
            return converterError(
                method,
                "[%s] is not a valid fully qualified method name: " +
                "it must start with a fully qualified class name followed by a '#' " +
                "and then the method name",
                converter
            );
        }

        String className = nameParts[0];
        TypeElement classElement = elementUtils.getTypeElement(className);
        if (classElement == null) {
            return converterError(
                method,
                "[%s] is not a valid fully qualified method name: " +
                "The class [%s] cannot be found",
                converter,
                className
            );
        }

        return memberDefinition(names, member, targetType, classElement, methodName, false);
    }

    private Optional<MemberDefinition> memberDefinition(
        NameAllocator names,
        ConfigParser.Member member,
        TypeMirror targetType,
        TypeElement classElement,
        CharSequence methodName,
        boolean scanInheritance
    ) {
        String converter = member.method().getAnnotation(ConvertWith.class).value();
        List<ExecutableElement> validCandidates = new ArrayList<>();
        Collection<InvalidCandidate> invalidCandidates = new ArrayList<>();
        Deque<TypeElement> classesToSearch = new ArrayDeque<>();
        classesToSearch.addLast(classElement);
        do {
            TypeElement currentClass = classesToSearch.pollFirst();
            assert currentClass != null;

            validCandidates.clear();

            for (ExecutableElement candidate : methodsIn(currentClass.getEnclosedElements())) {
                if (!candidate.getSimpleName().contentEquals(methodName)) {
                    continue;
                }

                int invalidMessages = invalidCandidates.size();

                Set<Modifier> modifiers = candidate.getModifiers();
                if (!modifiers.contains(Modifier.STATIC)) {
                    invalidCandidates.add(InvalidCandidate.of(candidate, "Must be static"));
                }
                if (!modifiers.contains(Modifier.PUBLIC)) {
                    invalidCandidates.add(InvalidCandidate.of(candidate, "Must be public"));
                }
                if (!candidate.getTypeParameters().isEmpty()) {
                    invalidCandidates.add(InvalidCandidate.of(candidate, "May not be generic"));
                }
                if (!candidate.getThrownTypes().isEmpty()) {
                    invalidCandidates.add(InvalidCandidate.of(candidate, "May not declare any exceptions"));
                }
                if (!(candidate.getParameters().size() == 1)) {
                    invalidCandidates.add(InvalidCandidate.of(candidate, "May only accept one parameter"));
                }
                if (!typeUtils.isAssignable(candidate.getReturnType(), targetType)) {
                    invalidCandidates.add(InvalidCandidate.of(
                        candidate,
                        "Must return a type that is assignable to %s",
                        targetType
                    ));
                }

                if (invalidCandidates.size() == invalidMessages) {
                    validCandidates.add(candidate);
                }
            }

            if (validCandidates.size() > 1) {
                for (ExecutableElement candidate : validCandidates) {
                    error(
                        String.format("Method is ambiguous and a possible candidate for [%s]", converter),
                        candidate
                    );
                }
                return converterError(
                    member.method(), "Multiple possible candidates found: %s", validCandidates
                );
            }

            if (validCandidates.size() == 1) {
                ExecutableElement candidate = validCandidates.get(0);
                VariableElement parameter = candidate.getParameters().get(0);
                TypeMirror currentType = currentClass.asType();
                return memberDefinition(names, member, parameter.asType())
                    .map(d -> ImmutableMemberDefinition.builder()
                        .from(d)
                        .addConverter(c -> CodeBlock.of(
                            "$T.$N($L)",
                            currentType,
                            candidate.getSimpleName().toString(),
                            c
                        ))
                        .build()
                    );
            }

            if (scanInheritance) {
                for (TypeMirror superInterface : currentClass.getInterfaces()) {
                    classesToSearch.addLast(asTypeElement(superInterface));
                }
            }
        } while (!classesToSearch.isEmpty());

        for (InvalidCandidate invalidCandidate : invalidCandidates) {
            error(String.format(invalidCandidate.message(), invalidCandidate.args()), invalidCandidate.element());
        }

        return converterError(
            member.method(),
            "No suitable method found that matches [%s]. " +
            "Make sure that the method is static, public, unary, not generic, " +
            "does not declare any exception and returns [%s]",
            converter,
            targetType
        );
    }

    private Optional<MemberDefinition> memberDefinition(
        NameAllocator names,
        ConfigParser.Member member,
        TypeMirror targetType
    ) {
        ImmutableMemberDefinition.Builder builder = ImmutableMemberDefinition
            .builder()
            .fieldName(names.get(member))
            .configParamName(names.get(CONFIG_VAR))
            .configKey(member.lookupKey())
            .fieldType(member.method().getReturnType())
            .parameterType(targetType)
            .methodPrefix("require");

        switch (targetType.getKind()) {
            case BOOLEAN:
                builder.methodName("Bool");
                break;
            case INT:
                builder.methodName("Int");
                break;
            case LONG:
                builder.methodName("Long");
                break;
            case DOUBLE:
                builder.methodName("Double");
                break;
            case BYTE:
            case SHORT:
            case FLOAT:
                builder
                    .methodName("Number")
                    .addConverter(c -> CodeBlock.of("$L.$LValue()", c, targetType));
                break;
            case DECLARED:
                if (isTypeOf(String.class, targetType)) {
                    builder.methodName("String");
                } else if (isTypeOf(Number.class, targetType)) {
                    builder.methodName("Number");
                } else {
                    builder
                        .methodName("Checked")
                        .expectedType(CodeBlock.of("$T.class", ClassName.get(asTypeElement(targetType))));
                }
                break;
            default:
                return error("Unsupported return type: " + targetType, member.method());
        }

        if (member.method().isDefault()) {
            builder
                .methodPrefix("get")
                .defaultProvider(CodeBlock.of(
                    "$T.super.$N()",
                    member.owner().asType(),
                    member.methodName()
                )
            );
        }

        return Optional.of(builder.build());
    }

    private Iterable<MethodSpec> defineGetters(ConfigParser.Spec config, NameAllocator names) {
        return config.members().stream().map(member -> MethodSpec
            .overriding(member.method())
            .addStatement("return this.$N", names.get(member))
            .build()
        ).collect(Collectors.toList());
    }

    private <T> Optional<T> error(CharSequence message, Element element) {
        messager.printMessage(
            Diagnostic.Kind.ERROR,
            message,
            element
        );
        return Optional.empty();
    }

    private <T> Optional<T> converterError(Element element, String message, Object... args) {
        messager.printMessage(
            Diagnostic.Kind.ERROR,
            String.format(message, args),
            element,
            getAnnotationMirror(element, ConvertWith.class).orNull()
        );
        return Optional.empty();
    }

    @ValueClass
    interface FieldDefinitions {
        List<FieldSpec> fields();

        NameAllocator names();
    }

    @ValueClass
    interface MemberDefinition {
        TypeMirror fieldType();

        TypeMirror parameterType();

        String fieldName();

        String configParamName();

        String methodPrefix();

        String methodName();

        String configKey();

        Optional<CodeBlock> defaultProvider();

        Optional<CodeBlock> expectedType();

        List<UnaryOperator<CodeBlock>> converters();
    }

    @ValueClass
    interface InvalidCandidate {
        Element element();

        String message();

        Object[] args();

        static InvalidCandidate of(Element element, String format, Object... args) {
            return ImmutableInvalidCandidate.builder().element(element).message(format).args(args).build();
        }
    }
}
