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

/**
 * A table containing all known symbols logically keyed by their identifier.
 */
class SymbolTable {

  /**
   * Represents a known thrift type.
   */
  @AutoValue
  abstract static class Symbol {
    static Symbol create(String packageName, Definition symbol) {
      return new AutoValue_SymbolTable_Symbol(packageName, symbol);
    }

    /**
     * @return The java package name of this symbol.
     */
    abstract String packageName();

    /**
     * @return The underlying thrift symbol (always a type; ie an enum, exception, service, struct
     *         or union).
     */
    abstract Definition symbol();

    /**
     * @return The fully qualified class name of this symbol.
     */
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

  /**
   * Equivalent to calling {@link #lookup(String, String)} with {@code identifier}'s name.
   *
   * @param packageName The package name of the requestor.
   * @param identifier The type identifier.
   * @return The identified symbol.
   * @throws ParseException if the symbol could not be found.
   */
  Symbol lookup(String packageName, IdentifierType identifier) throws ParseException {
    return lookup(packageName, identifier.getName());
  }

  /**
   * Looks up the given identifier.
   * <p>
   * The {@code identifierName}s come in 2 forms:
   * <ol>
   *   <li>LocalType</li>
   *   <li>included.Type</li>
   * </ol>
   * <p>
   * The first form is used to refer to a thrift type declared in the same thrift file.  For these
   * local types the given {@code packageName} is used to lookup the symbol.
   * <p>
   * The second form is used to refer to types included from other thrift files.  For these included
   * types the local {@code packageName} is ignored.
   *
   * @param packageName The package name of the requestor.
   * @param identifierName The type identifier.
   * @return The identified symbol.
   * @throws ParseException if the symbol could not be found.
   */
  Symbol lookup(String packageName, String identifierName) throws ParseException {
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

  /**
   * Creates a new symbol table containing this symbol table's definitions as well as all the the
   * definitions from the given file.
   *
   * @param file A thrift file containing definitions to add to the new symbol table.
   * @param packageName The package name for types defined in the given {@code file}.
   * @param definitions The thrift definitions contained in the given {@code file}.
   * @return A new symbol table containing the union of this symbol table's definitions with the
   *         new definitions from {@code file}.
   */
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
}
