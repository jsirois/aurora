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

import com.facebook.swift.parser.model.Definition;
import com.facebook.swift.parser.model.IdentifierType;
import com.google.auto.value.AutoValue;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.squareup.javapoet.ClassName;

class SymbolTable {
  @AutoValue
  abstract static class Symbol {
    static Symbol create(String packageName, Definition symbol) {
      return new AutoValue_SymbolTable_Symbol(packageName, symbol);
    }

    abstract String packageName();
    abstract Definition symbol();

    ClassName getClassName() {
      return ClassName.get(packageName(), symbol().getName());
    }
  }

  private final ImmutableBiMap<File, String> importPrefixByFile;
  private final ImmutableMap<String, String> packageNameByImportPrefix;
  private final ImmutableMap<String, ImmutableMap<String, Symbol>> symbolsByPackageName;

  SymbolTable() {
    this(
        ImmutableBiMap.<File, String>of(),
        ImmutableMap.<String, String>of(),
        ImmutableMap.<String, ImmutableMap<String, Symbol>>of());
  }

  private SymbolTable(
      ImmutableBiMap<File, String> importPrefixByFile,
      ImmutableMap<String, String> packageNameByImportPrefix,
      ImmutableMap<String, ImmutableMap<String, Symbol>> symbolsByPackageName) {

    this.importPrefixByFile = importPrefixByFile;
    this.packageNameByImportPrefix = packageNameByImportPrefix;
    this.symbolsByPackageName = symbolsByPackageName;
  }

  Symbol lookup(String packageName, IdentifierType identifier) {
    return lookup(packageName, identifier.getName());
  }

  Symbol lookup(String packageName, String identifierName) {
    List<String> parts = Splitter.on('.').limit(2).splitToList(identifierName);
    if (parts.size() == 2) {
      String importPrefix = parts.get(0);
      packageName = packageNameByImportPrefix.get(importPrefix);
      if (packageName == null) {
        throw new ParseException(
            String.format(
                "Could not map identifier %s to a parsed type: %s", identifierName, this));
      }
      identifierName = parts.get(1);
    }
    return symbolsByPackageName.get(packageName).get(identifierName);
  }

  SymbolTable updated(File file, String packageName, Iterable<Definition> definitions) {
    if (importPrefixByFile.containsKey(file)) {
      return this;
    }

    String importPrefix = Files.getNameWithoutExtension(file.getName());
    String existingPackageName = packageNameByImportPrefix.get(importPrefix);
    if (existingPackageName != null) {
      throw new ParseException(
          String.format(
              "Invalid include, already have an include with prefix of %s containing " +
                  "definitions for package %s in file %s.",
              importPrefix, existingPackageName, importPrefixByFile.inverse().get(importPrefix)));
    }

    ImmutableBiMap<File, String> prefixByFile =
        ImmutableBiMap.<File, String>builder()
            .putAll(importPrefixByFile)
            .put(file, importPrefix)
            .build();

    ImmutableMap<String, String> packageByPrefix =
        ImmutableMap.<String, String>builder()
            .putAll(packageNameByImportPrefix)
            .put(importPrefix, packageName)
            .build();

    ImmutableMap<String, ImmutableMap<String, Symbol>> symbolsByPackage =
        ImmutableMap.<String, ImmutableMap<String, Symbol>>builder()
            .putAll(symbolsByPackageName)
            .put(
                packageName,
                Maps.uniqueIndex(
                    Iterables.transform(definitions, d -> Symbol.create(packageName, d)),
                    s -> s.getClassName().simpleName()))
            .build();

    return new SymbolTable(prefixByFile, packageByPrefix, symbolsByPackage);
  }

  @Override
  public String toString() {
    return "SymbolTable{" +
        "packageNameByImportPrefix=" + packageNameByImportPrefix +
        ", symbolsByPackageName=" + symbolsByPackageName +
        '}';
  }
}
