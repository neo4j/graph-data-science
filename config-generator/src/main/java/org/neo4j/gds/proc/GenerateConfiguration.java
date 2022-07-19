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

import com.google.auto.common.GeneratedAnnotationSpecs;
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
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.Configuration.ConvertWith;
import org.neo4j.gds.annotation.Configuration.Parameter;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;

import javax.annotation.processing.Messager;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.auto.common.MoreElements.asType;
import static com.google.auto.common.MoreElements.getAnnotationMirror;
import static com.google.auto.common.MoreTypes.asTypeElement;
import static com.google.auto.common.MoreTypes.isTypeOf;
import static javax.lang.model.type.TypeKind.DECLARED;
import static javax.lang.model.util.ElementFilter.methodsIn;
import static org.neo4j.gds.proc.GenerateConfigurationMethods.defineMemberMethods;

final class GenerateConfiguration {

    private static final String CONFIG_VAR = "config";
    private static final String INSTANCE_VAR = "instance";
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
        return JavaFile
            .builder(packageName, typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
    }

    private TypeSpec process(ConfigParser.Spec config, String packageName, String generatedClassName) {
        TypeSpec.Builder builder = classBuilder(config, packageName, generatedClassName);

        FieldDefinitions fieldDefinitions = defineFields(config);
        builder.addFields(fieldDefinitions.fields());

        String configParameterName = fieldDefinitions.names().newName(CONFIG_VAR, CONFIG_VAR);
        List<MemberDefinition> implMembers =  config.members().stream()
            .flatMap(parserMember -> memberDefinition(fieldDefinitions.names(), parserMember).stream())
            .collect(Collectors.toList());

        var validationMethods = config.members().stream()
            .filter(ConfigParser.Member::validates)
            .map(ConfigParser.Member::methodName);

        MethodSpec constructor = defineConstructor(implMembers, validationMethods, fieldDefinitions.names());
        Optional<MethodSpec> factory = defineFactory(config, generatedClassName, constructor, fieldDefinitions.names());

        if (factory.isPresent()) {
            MethodSpec privateConstructor = MethodSpec.constructorBuilder()
                .addAnnotations(constructor.annotations)
                .addParameters(constructor.parameters)
                .addCode(constructor.code)
                .addModifiers(Modifier.PRIVATE)
                .build();
            builder.addMethod(privateConstructor);
            builder.addMethod(factory.get());
        } else {
            builder.addMethod(constructor);
        }

        ClassName builderClassName = ClassName.get(packageName, generatedClassName + ".Builder");
        TypeSpec configBuilderClass = GenerateConfigurationBuilder.defineConfigBuilder(
            TypeName.get(config.rootType()),
            implMembers,
            builderClassName,
            generatedClassName,
            constructor.parameters,
            configParameterName,
            factory
        );

        MethodSpec builderFactoryMethod = MethodSpec.methodBuilder("builder")
            .addModifiers(Modifier.STATIC, Modifier.PUBLIC)
            .addStatement("return new $T()", builderClassName)
            .returns(builderClassName)
            .build();

        return builder
            .addMethods(defineMemberMethods(config, fieldDefinitions.names()))
            .addMethod(builderFactoryMethod)
            .addType(configBuilderClass)
            .build();
    }

    private TypeSpec.Builder classBuilder(ConfigParser.Spec config, String packageName, String generatedClassName) {
        TypeSpec.Builder classBuilder = TypeSpec
            .classBuilder(ClassName.get(packageName, generatedClassName))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addOriginatingElement(config.root())
            .addSuperinterface(TypeName.get(config.rootType()));

        GeneratedAnnotationSpecs
            .generatedAnnotationSpec(elementUtils, sourceVersion, ConfigurationProcessor.class)
            .ifPresent(classBuilder::addAnnotation);

        return classBuilder;
    }

    private static FieldDefinitions defineFields(ConfigParser.Spec config) {
        NameAllocator names = new NameAllocator();
        ImmutableFieldDefinitions.Builder builder = ImmutableFieldDefinitions.builder().names(names);
        config.members().stream().filter(ConfigParser.Member::isConfigValue).map(member ->
            FieldSpec.builder(
                member.typeSpecWithAnnotation(Nullable.class),
                names.newName(member.methodName(), member),
                Modifier.PRIVATE
            ).build()
        ).forEach(builder::addField);
        return builder.build();
    }

    private MethodSpec defineConstructor(
        List<MemberDefinition> implMembers,
        Stream<String> validationMethods,
        NameAllocator names
    ) {
        MethodSpec.Builder constructorMethodBuilder = MethodSpec
            .constructorBuilder()
            .addModifiers(Modifier.PUBLIC);

        boolean requiredMapParameter = false;

        String errorsVarName = names.newName("errors");
        if (!implMembers.isEmpty()) {
            constructorMethodBuilder.addStatement(
                "$1T<$2T> $3N = new $1T<>()",
                ArrayList.class,
                IllegalArgumentException.class,
                errorsVarName
            );
        }

        for (MemberDefinition implMember : implMembers) {
            var parsedMember = implMember.member();

            if (parsedMember.isConfigMapEntry()) {
                requiredMapParameter = true;
                addConfigFieldToConstructor(constructorMethodBuilder, implMember, errorsVarName);
            } else {
                addParameterToConstructor(
                    constructorMethodBuilder,
                    implMember,
                    errorsVarName
                );
            }
        }

        validationMethods.forEach(method -> catchValidationError(
            constructorMethodBuilder,
            errorsVarName,
            builder -> builder.addStatement("$N()", method)
        ));

        if (!implMembers.isEmpty()) {
            ErrorPropagator.combineCollectedErrors(names, constructorMethodBuilder, errorsVarName);
        }

        if (requiredMapParameter) {
            constructorMethodBuilder.addParameter(
                TypeName.get(CypherMapWrapper.class).annotated(NOT_NULL),
                names.get(CONFIG_VAR)
            );
        }

        return constructorMethodBuilder.build();
    }

    private void catchValidationError(
        MethodSpec.Builder builder,
        String errorVarName,
        UnaryOperator<MethodSpec.Builder> statementFunc
    ) {
        builder.beginControlFlow("try");
        statementFunc.apply(builder);
        builder
            .nextControlFlow("catch ($T e)", IllegalArgumentException.class)
            .addStatement("$N.add(e)", errorVarName)
            // should only throw NPE if previously an error occured on the field it valides on (field is null then)
            .nextControlFlow("catch ($T e)", NullPointerException.class)
            .endControlFlow();
    }

    private Optional<MethodSpec> defineFactory(
        ConfigParser.Spec config,
        String generatedClassName,
        MethodSpec constructor,
        NameAllocator names
    ) {
        List<ConfigParser.Member> normalizers = config
            .members()
            .stream()
            .filter(ConfigParser.Member::normalizes)
            .collect(Collectors.toList());

        if (normalizers.isEmpty()) {
            return Optional.empty();
        }

        CodeBlock constructorArgs = constructor.parameters
            .stream()
            .map(param -> CodeBlock.of("$N", param))
            .collect(CodeBlock.joining(", "));

        String instanceVarName = names.newName(INSTANCE_VAR, INSTANCE_VAR);
        TypeName interfaceType = TypeName.get(config.rootType());

        MethodSpec.Builder factory = MethodSpec
            .methodBuilder("of")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(interfaceType)
            .addParameters(constructor.parameters)
            .addStatement("$T $N = new $L($L)", interfaceType, instanceVarName, generatedClassName, constructorArgs);

        for (ConfigParser.Member member : normalizers) {
            factory.addStatement("$1N = $1N.$2N()", instanceVarName, member.methodName());
        }

        return Optional.of(factory
            .addStatement("return $N", instanceVarName)
            .build());
    }

    private void addConfigFieldToConstructor(
        MethodSpec.Builder constructor,
        MemberDefinition definition,
        String errorsVarName
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
        TypeMirror fieldType = definition.fieldType();
        if (fieldType.getKind() == DECLARED) {
            boolean isNullable = !definition.member().annotations(Nullable.class).isEmpty();

            if (!isNullable) {
                codeBlock = CodeBlock.of(
                    "$T.failOnNull($S, $L)",
                    CypherMapWrapper.class,
                    definition.configKey(),
                    codeBlock
                );
            }
        }

        var fieldCodeBuilder = CodeBlock.builder().addStatement("this.$N = $L", definition.fieldName(), codeBlock);

        addValidationCode(definition, fieldCodeBuilder);

        ErrorPropagator.catchAndPropagateIllegalArgumentError(
            constructor,
            errorsVarName,
            builder -> builder.addCode(fieldCodeBuilder.build())
        );
    }

    private void addValidationCode(MemberDefinition definition, CodeBlock.Builder fieldCodeBuilder) {
        Consumer<CodeBlock> validationConsumer = fieldCodeBuilder::addStatement;
        if (isTypeOf(Optional.class, definition.fieldType())) {
            validationConsumer = validatorCode -> fieldCodeBuilder.addStatement(
                "$1L.ifPresent($1L -> $2L)",
                definition.fieldName(),
                validatorCode
            );
        } else if (isTypeOf(List.class, definition.fieldType())){
            validationConsumer = validatorCode -> fieldCodeBuilder.addStatement(
                "this.$1N.forEach($1N -> $2L)",
                definition.fieldName(),
                validatorCode
            );
        }

        if (definition.member().validatesIntegerRange()) {
            Configuration.IntegerRange range = definition
                .member()
                .method()
                .getAnnotation(Configuration.IntegerRange.class);
            validationConsumer.accept(CodeBlock.of(
                "$T.validateIntegerRange($S, $L, $L, $L, $L, $L)",
                CypherMapWrapper.class,
                definition.configKey(),
                definition.fieldName(),
                elementUtils.getConstantExpression(range.min()),
                elementUtils.getConstantExpression(range.max()),
                elementUtils.getConstantExpression(range.minInclusive()),
                elementUtils.getConstantExpression(range.maxInclusive())
            ));
        }

        if (definition.member().validatesLongRange()) {
            Configuration.LongRange range = definition
                .member()
                .method()
                .getAnnotation(Configuration.LongRange.class);
            validationConsumer.accept(CodeBlock.of(
                "$T.validateLongRange($S, $L, $L, $L, $L, $L)",
                CypherMapWrapper.class,
                definition.configKey(),
                definition.fieldName(),
                elementUtils.getConstantExpression(range.min()),
                elementUtils.getConstantExpression(range.max()),
                elementUtils.getConstantExpression(range.minInclusive()),
                elementUtils.getConstantExpression(range.maxInclusive())
            ));
        }

        if (definition.member().validatesDoubleRange()) {
            Configuration.DoubleRange range = definition
                .member()
                .method()
                .getAnnotation(Configuration.DoubleRange.class);
            validationConsumer.accept(CodeBlock.of(
                "$T.validateDoubleRange($S, $L, $L, $L, $L, $L)",
                CypherMapWrapper.class,
                definition.configKey(),
                definition.fieldName(),
                elementUtils.getConstantExpression(range.min()),
                elementUtils.getConstantExpression(range.max()),
                elementUtils.getConstantExpression(range.minInclusive()),
                elementUtils.getConstantExpression(range.maxInclusive())
            ));
        }
    }

    private void addParameterToConstructor(
        MethodSpec.Builder constructor,
        MemberDefinition definition,
        String errorsVarName
    ) {
        TypeName paramType = TypeName.get(definition.parameterType());

        CodeBlock valueProducer;
        if (definition.parameterType().getKind() == DECLARED) {
            if (definition.member().method().getAnnotation(Parameter.class).acceptNull()) {
                paramType = paramType.annotated(NULLABLE);
                valueProducer = CodeBlock.of("$N", definition.fieldName());
            } else {
                paramType = paramType.annotated(NOT_NULL);
                valueProducer = CodeBlock.of(
                    "$T.failOnNull($S, $N)",
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

        CodeBlock finalValueProducer = valueProducer;
        constructor.addParameter(paramType, definition.fieldName());
        ErrorPropagator.catchAndPropagateIllegalArgumentError(
            constructor,
            errorsVarName,
            builder -> builder.addStatement("this.$N = $L", definition.fieldName(), finalValueProducer)
        );
    }

    private Optional<MemberDefinition> memberDefinition(NameAllocator names, ConfigParser.Member member) {
        if (!member.isConfigValue()) {
            return Optional.empty();
        }

        ExecutableElement method = member.method();
        TypeMirror targetType = method.getReturnType();
        ConvertWith convertWith = method.getAnnotation(ConvertWith.class);
        if (convertWith == null) {
            return memberDefinition(names, member, targetType);
        }

        String converter = convertWith.value().trim();
        if (converter.isEmpty()) {
            return converterError(method, "Empty conversion method is not allowed.");
        }

        if (!converter.contains("#")) {
            if (!SourceVersion.isIdentifier(converter)) {
                return converterError(
                    method,
                    "[%s] is not a valid identifier, see the @Configuration.ConvertWith documentation.",
                    converter
                );
            }
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
                "and then the method name.",
                converter
            );
        }

        String className = nameParts[0];
        TypeElement classElement = elementUtils.getTypeElement(className);
        if (classElement == null) {
            return converterError(
                method,
                "[%s] is not a valid fully qualified method name: " +
                "The class [%s] cannot be found.",
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
            if (currentClass == null) {
                return converterError(classElement, "Inherited interface was null, this could be a bug in the JDK.");
            }

            validCandidates.clear();

            for (ExecutableElement candidate : methodsIn(currentClass.getEnclosedElements())) {
                if (!candidate.getSimpleName().contentEquals(methodName)) {
                    continue;
                }

                int sizeBeforeValidation = invalidCandidates.size();

                validateCandidateModifiers(targetType, invalidCandidates, candidate, candidate.getModifiers());

                if (invalidCandidates.size() == sizeBeforeValidation) {
                    validCandidates.add(candidate);
                }
            }

            if (validCandidates.size() > 1) {
                for (ExecutableElement candidate : validCandidates) {
                    error(
                        String.format(Locale.ENGLISH,"Method is ambiguous and a possible candidate for [%s].", converter),
                        candidate
                    );
                }
                return converterError(member.method(), "Multiple possible candidates found: %s.", validCandidates);
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
            error(String.format(Locale.ENGLISH,invalidCandidate.message(), invalidCandidate.args()), invalidCandidate.element());
        }

        return converterError(
            member.method(),
            "No suitable method found that matches [%s]. " +
            "Make sure that the method is static, public, unary, not generic, " +
            "does not declare any exception and returns [%s].",
            converter,
            targetType
        );
    }

    private void validateCandidateModifiers(
        TypeMirror targetType,
        Collection<InvalidCandidate> invalidCandidates,
        ExecutableElement candidate,
        Set<Modifier> modifiers
    ) {
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
    }

    private Optional<MemberDefinition> memberDefinition(
        NameAllocator names,
        ConfigParser.Member member,
        TypeMirror targetType
    ) {
        ImmutableMemberDefinition.Builder builder = ImmutableMemberDefinition
            .builder()
            .member(member)
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
                } else if (isTypeOf(Optional.class, targetType)) {
                    Optional<ClassName> maybeInnerType;
                    if (member.method().isDefault()) {
                        maybeInnerType = error(
                            "Optional fields can not to be declared default (Optional.empty is the default).",
                            member.method()
                        );
                    } else {
                        List<? extends TypeMirror> typeArguments = ((DeclaredType) targetType).getTypeArguments();
                        if (typeArguments.isEmpty()) {
                            maybeInnerType = error(
                                "Optional must have a Cypher-supported type as type argument, but found none.",
                                member.method()
                            );
                        } else {
                            maybeInnerType = Optional.of(ClassName.get(asTypeElement(typeArguments.get(0))));
                        }
                    }

                    if (maybeInnerType.isPresent()) {
                        builder
                            .methodPrefix("get")
                            .methodName("Optional")
                            .expectedType(CodeBlock.of("$T.class", maybeInnerType.get()));
                    } else {
                        return Optional.empty();
                    }
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
            String.format(Locale.ENGLISH, message, args),
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
        ConfigParser.Member member();

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
