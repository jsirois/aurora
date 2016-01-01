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
package org.apache.aurora.build.thrift;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.lang.model.element.Modifier;

import com.facebook.swift.parser.model.Const;
import com.facebook.swift.parser.model.ThriftType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import org.slf4j.Logger;

@NotThreadSafe
class ConstVisitor extends BaseVisitor<Const> {
  private final ImmutableList.Builder<Const> consts = ImmutableList.builder();

  ConstVisitor(Logger logger, File outdir, SymbolTable symbolTable, String packageName) {
    super(logger, outdir, symbolTable, packageName);
  }

  @Override
  public void visit(Const constant) {
    consts.add(constant);
  }

  @Override
  public void finish(ImmutableMap<String, AbstractStructRenderer> structRenderers)
      throws IOException {

    ImmutableList<Const> constants = consts.build();
    if (constants.isEmpty()) {
      return;
    }

    TypeSpec.Builder typeBuilder =
        TypeSpec.classBuilder("Constants")
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build());

    for (Const constant : constants) {
      ThriftType fieldType = constant.getType();
      typeBuilder.addField(
          FieldSpec.builder(
              typeName(fieldType),
              constant.getName(),
              Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
              .initializer(renderValue(structRenderers, fieldType, constant.getValue()))
              .build());
    }
    writeType(typeBuilder);
  }
}
