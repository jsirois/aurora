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

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.lang.model.element.Modifier;

import com.facebook.swift.parser.model.AbstractStruct;
import com.facebook.swift.parser.model.IdentifierType;
import com.facebook.swift.parser.model.ListType;
import com.facebook.swift.parser.model.SetType;
import com.facebook.swift.parser.model.Struct;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftType;
import com.google.common.base.Optional;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;

import org.slf4j.Logger;

// TODO(John Sirois): XXX MutablePeer is not actually a Visitor, it just uses baseclass
// functionality.  Refactor BaseVisitor type emitter helpers to a non-visitor base or util.
class MutablePeer extends BaseVisitor {
  MutablePeer(Logger logger, File outdir, SymbolTable symbolTable, String packageName) {
    super(logger, outdir, symbolTable, packageName);
  }

  TypeSpec render(Struct struct, TypeSpec typeSpec, PeerInfo peerInfo) {
    TypeSpec.Builder typeBuilder =
        TypeSpec.classBuilder(peerInfo.className)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build());

    CodeBlock.Builder toThriftCode =
        CodeBlock.builder()
            .add("$[return $N.builder()", typeSpec);

    for (ThriftField field : struct.getFields()) {
      ThriftType fieldType = field.getType();
      FieldSpec fieldSpec =
          FieldSpec.builder(typeName(fieldType), field.getName())
              .addModifiers(Modifier.PRIVATE)
              .build();
      CodeBlock code = CodeBlock.builder().add("$N", fieldSpec).build();

      toThriftCode.add("\n.$L(", setterName(field));
      if (fieldType instanceof IdentifierType) {
        SymbolTable.Symbol symbol = lookup(((IdentifierType) fieldType));
        if (symbol.getSymbol() instanceof AbstractStruct) {
          Optional<PeerInfo> peer =
              PeerInfo.from(symbol.getPackageName(), (AbstractStruct) symbol.getSymbol());
          if (peer.isPresent()) {
            ClassName peerType = ClassName.get(peer.get().packageName, peer.get().className);
            fieldSpec =
                FieldSpec.builder(peerType, field.getName())
                    .addModifiers(Modifier.PRIVATE)
                    .build();
            code =
                CodeBlock.builder()
                    .add("$N == null ? null : $N.toThrift()", fieldSpec, fieldSpec)
                    .build();
          }
        }
      } else if (fieldType instanceof ListType) {
        ThriftType elementType = ((ListType) fieldType).getElementType();
        if (elementType instanceof IdentifierType) {
          SymbolTable.Symbol symbol = lookup(((IdentifierType) elementType));
          if (symbol.getSymbol() instanceof AbstractStruct) {
            Optional<PeerInfo> peer =
                PeerInfo.from(symbol.getPackageName(), (AbstractStruct) symbol.getSymbol());
            if (peer.isPresent()) {
              ClassName peerType = ClassName.get(peer.get().packageName, peer.get().className);
              ParameterizedTypeName listType =
                  ParameterizedTypeName.get(ClassName.get(List.class), peerType);
              fieldSpec =
                  FieldSpec.builder(listType, field.getName())
                      .addModifiers(Modifier.PRIVATE)
                      .build();
              code =
                  CodeBlock.builder()
                      .add(
                          "$N.stream().map($T::toThrift).collect($T.toList())",
                          fieldSpec,
                          peerType,
                          Collectors.class)
                      .build();
            }
          }
        } else {
          ParameterizedTypeName listType =
              ParameterizedTypeName.get(ClassName.get(List.class), typeName(elementType));
          fieldSpec =
              FieldSpec.builder(listType, field.getName())
                  .addModifiers(Modifier.PRIVATE)
                  .build();
        }
      } else if (fieldType instanceof SetType) {
        ThriftType elementType = ((SetType) fieldType).getElementType();
        if (elementType instanceof IdentifierType) {
          SymbolTable.Symbol symbol = lookup(((IdentifierType) elementType));
          if (symbol.getSymbol() instanceof AbstractStruct) {
            Optional<PeerInfo> peer =
                PeerInfo.from(symbol.getPackageName(), (AbstractStruct) symbol.getSymbol());
            if (peer.isPresent()) {
              ClassName peerType = ClassName.get(peer.get().packageName, peer.get().className);
              ParameterizedTypeName setType =
                  ParameterizedTypeName.get(ClassName.get(Set.class), peerType);
              fieldSpec =
                  FieldSpec.builder(setType, field.getName())
                      .addModifiers(Modifier.PRIVATE)
                      .build();
              code =
                  CodeBlock.builder()
                      .add(
                          "$N.stream().map($T::toThrift).collect($T.toSet())",
                          fieldSpec,
                          peerType,
                          Collectors.class)
                      .build();
            }
          }
        } else {
          ParameterizedTypeName listType =
              ParameterizedTypeName.get(ClassName.get(Set.class), typeName(elementType));
          fieldSpec =
              FieldSpec.builder(listType, field.getName())
                  .addModifiers(Modifier.PRIVATE)
                  .build();
        }
      } // TODO(John Sirois): XXX Handle maps

      typeBuilder.addField(fieldSpec);
      toThriftCode.add(code);
      toThriftCode.add(")");
    }

    typeBuilder.addMethod(
        MethodSpec.methodBuilder("toThrift")
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .returns(getClassName(typeSpec.name))
            .addCode(
                toThriftCode
                    .add("\n.build();\n$]")
                    .build())
            .build());

    return typeBuilder.build();
  }
}
