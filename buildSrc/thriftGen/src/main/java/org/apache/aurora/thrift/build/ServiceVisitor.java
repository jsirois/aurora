/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.thrift.build;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.lang.model.element.Modifier;

import com.facebook.swift.parser.model.Service;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftMethod;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;

import org.apache.aurora.thrift.ThriftService;
import org.slf4j.Logger;

@NotThreadSafe
class ServiceVisitor extends BaseVisitor<Service> {
  private static final ParameterizedTypeName CLASS_TYPE =
      ParameterizedTypeName.get(
          ClassName.get(Class.class),
          WildcardTypeName.subtypeOf(Object.class));

  private static final ArrayTypeName ARRAY_OF_CLASS_TYPE = ArrayTypeName.of(CLASS_TYPE);

  private static final ParameterizedTypeName METHODS_MAP_TYPE =
      ParameterizedTypeName.get(
          ClassName.get(ImmutableMap.class),
          ClassName.get(String.class),
          ARRAY_OF_CLASS_TYPE);

  ServiceVisitor(Logger logger, Path outdir, SymbolTable symbolTable, String packageName) {
    super(logger, outdir, symbolTable, packageName);
  }

  @Override
  public void visit(Service service) throws IOException {
    TypeSpec.Builder serviceContainerBuilder =
        TypeSpec.interfaceBuilder(service.getName())
            .addModifiers(Modifier.PUBLIC);

    CodeBlock.Builder methodsMapInitializerCode =
        CodeBlock.builder()
            .add(
                "$[$T.<$T, $T>builder()",
                ImmutableMap.class,
                String.class,
                ARRAY_OF_CLASS_TYPE);

    TypeSpec.Builder asyncServiceBuilder = createServiceBuilder(service, "Async");
    TypeSpec.Builder syncServiceBuilder = createServiceBuilder(service, "Sync");

    Optional<String> parent = service.getParent();
    if (parent.isPresent()) {
      methodsMapInitializerCode.add(
          "\n.putAll($T._METHOD_PARAMETER_TYPES)", getClassName(parent.get()));
      asyncServiceBuilder.addSuperinterface(getClassName(parent.get(), "Async"));
      syncServiceBuilder.addSuperinterface(getClassName(parent.get(), "Sync"));
    }

    for (ThriftMethod method : service.getMethods()) {
      methodsMapInitializerCode
          .add("\n.put($S,\n", method.getName())
          .indent()
          .indent()
          .add(renderParameterMapInitializer(method))
          .unindent()
          .unindent()
          .add(")");

      asyncServiceBuilder.addMethod(
          renderMethod(
              method,
              parameterizedTypeName(
                  ListenableFuture.class,
                  /* mutable */ true,
                  method.getReturnType())));
      syncServiceBuilder.addMethod(
          renderMethod(method, typeName(method.getReturnType(), /* mutable */ true)));
    }

    CodeBlock methodMapInitializer =
        methodsMapInitializerCode
            .add("\n.build()$]")
            .build();

    FieldSpec methodsField =
        FieldSpec.builder(METHODS_MAP_TYPE, "_METHOD_PARAMETER_TYPES")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
            .initializer(methodMapInitializer)
            .build();
    serviceContainerBuilder.addField(methodsField);

    ClassName asyncServiceName = getClassName(service.getName(), "Async");
    addMetadataMethods(asyncServiceBuilder, asyncServiceName, methodsField);
    serviceContainerBuilder.addType(asyncServiceBuilder.build());

    ClassName syncServiceName = getClassName(service.getName(), "Sync");
    addMetadataMethods(syncServiceBuilder, syncServiceName, methodsField);
    serviceContainerBuilder.addType(syncServiceBuilder.build());

    writeType(serviceContainerBuilder);
  }

  private void addMetadataMethods(
      TypeSpec.Builder serviceBuilder,
      ClassName className,
      FieldSpec methodsField) {

    ParameterizedTypeName loadingCacheType =
        ParameterizedTypeName.get(LoadingCache.class, String.class, Method.class);

    TypeSpec cacheLoader =
        TypeSpec.anonymousClassBuilder("")
            .superclass(ParameterizedTypeName.get(CacheLoader.class, String.class, Method.class))
            .addMethod(
                MethodSpec.methodBuilder("load")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(String.class, "methodName")
                    .returns(Method.class)
                    .addException(NoSuchMethodException.class)
                    .addStatement(
                        "$T parameterTypes = $N.get(methodName)",
                        ARRAY_OF_CLASS_TYPE,
                        methodsField)
                    .beginControlFlow("if (parameterTypes == null)")
                    .addStatement("throw new $T(methodName)", NoSuchMethodException.class)
                    .endControlFlow()
                    .addStatement(
                        "return $T.class.getMethod(methodName, parameterTypes)",
                        className)
                    .build())
            .build();

    FieldSpec methodCacheField =
        FieldSpec.builder(loadingCacheType, "_METHODS")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
            .initializer("$T.newBuilder().build(\n$>$>$L)$<$<", CacheBuilder.class, cacheLoader)
            .build();
    serviceBuilder.addField(methodCacheField);

    serviceBuilder.addMethod(
        MethodSpec.methodBuilder("thriftMethod")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(String.class, "methodName")
            .returns(Method.class)
            .addException(NoSuchMethodException.class)
            .beginControlFlow("try")
            .addStatement("return $N.get(methodName)", methodCacheField)
            .nextControlFlow("catch ($T e)", ExecutionException.class)
            .addStatement("$T cause = e.getCause()", Throwable.class)
            .addStatement(
                "$T.propagateIfInstanceOf(cause, $T.class)",
                Throwables.class,
                NoSuchMethodException.class)
            .addStatement("throw new $T(cause)", IllegalStateException.class)
            .endControlFlow()
            .build());

    MethodSpec thriftMethods =
        MethodSpec.methodBuilder("thriftMethods")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(ParameterizedTypeName.get(ImmutableMap.class, String.class, Method.class))
            .beginControlFlow("try")
            .addStatement("return $N.getAll($N.keySet())", methodCacheField, methodsField)
            .nextControlFlow("catch ($T e)", ExecutionException.class)
            .addStatement("throw new $T(e.getCause())", IllegalStateException.class)
            .endControlFlow()
            .build();
    serviceBuilder.addMethod(thriftMethods);

    serviceBuilder.addMethod(
        MethodSpec.methodBuilder("getThriftMethods")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(thriftMethods.returnType)
            .addStatement("return $N()", thriftMethods)
            .build());
  }

  private CodeBlock renderParameterMapInitializer(ThriftMethod method) {
    if (method.getArguments().isEmpty()) {
      return CodeBlock.builder().add("new $T[0]", CLASS_TYPE).build();
    }

    CodeBlock.Builder parameterMapInitializerCode =
        CodeBlock.builder()
            .add("new $T[] {", CLASS_TYPE)
            .indent()
            .indent();

    for (ThriftField field : method.getArguments()) {
      TypeName fieldType = typeName(field.getType(), /* mutable */ true);
      if (fieldType instanceof ParameterizedTypeName) {
        fieldType = ((ParameterizedTypeName) fieldType).rawType;
      }
      parameterMapInitializerCode.add("\n$T.class,", fieldType);
    }

    return parameterMapInitializerCode
        .add("\n}")
        .unindent()
        .unindent()
        .build();
  }

  private TypeSpec.Builder createServiceBuilder(Service service, String typeName)
      throws IOException {

    return TypeSpec.interfaceBuilder(typeName)
        .addAnnotation(
            AnnotationSpec.builder(com.facebook.swift.service.ThriftService.class)
                .addMember("value", "$S", service.getName())
                .build())
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .addSuperinterface(ThriftService.class)
        .addMethod(MethodSpec.methodBuilder("close")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
            .build());
  }

  private MethodSpec renderMethod(ThriftMethod method, TypeName returnType) {
    if (!method.getThrowsFields().isEmpty()) {
      throw new UnsupportedFeatureException("Service methods that declare exceptions are not " +
          "supported, given " + method);
    }

    MethodSpec.Builder methodBuilder =
        MethodSpec.methodBuilder(method.getName())
            .addAnnotation(
                AnnotationSpec.builder(com.facebook.swift.service.ThriftMethod.class)
                    .addMember("value", "$S", method.getName())
                    .addMember("oneway", "$L", method.isOneway())
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(returnType);

    for (ThriftField field : method.getArguments()) {
      ParameterSpec.Builder paramBuilder =
          ParameterSpec.builder(typeName(field.getType(), /* mutable */ true), field.getName())
              .addAnnotation(renderThriftFieldAnnotation(field));
      if (!field.getAnnotations().isEmpty()) {
        paramBuilder.addAnnotation(
            BaseVisitor.createAnnotation(field.getAnnotations()));
      }
      if (field.getRequiredness() != ThriftField.Requiredness.REQUIRED) {
        paramBuilder.addAnnotation(javax.annotation.Nullable.class);
      }
      methodBuilder.addParameter(paramBuilder.build());
    }

    return methodBuilder.build();
  }
}
