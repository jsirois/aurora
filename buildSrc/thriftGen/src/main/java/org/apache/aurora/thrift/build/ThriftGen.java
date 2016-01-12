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
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.UnaryOperator;

import com.facebook.swift.parser.ThriftIdlParser;
import com.facebook.swift.parser.model.Document;
import com.google.common.base.Charsets;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharSource;

import org.apache.aurora.thrift.ThriftAnnotation;
import org.slf4j.Logger;

import autovalue.shaded.com.google.common.common.collect.Iterables;

import static java.util.Objects.requireNonNull;

/**
 * A thrift code generator for java.
 * <p>
 * This generator produces java thrift stubs that serialized and deserialize in a wire-compatible
 * way with apache thrift via the
 * <a href="https://github.com/facebook/swift/tree/master/swift-codec">swift-codec</a> library.
 * This code generator provides 2 primary advantages over the apache thrift code generator at this
 * time:
 * <ul>
 *   <li>
 *     <em>Immutable Structs and Unions</em>
 *     <p>
 *     The generated structs and unions are immutable.  To create a new struct a nested builder type
 *     must be instantiated, mutated, and then used to create the struct.  Any struct collections
 *     fields are represented with guava {@link ImmutableList}, {@link ImmutableSet} and
 *     {@link ImmutableMap} and the remaining types are all naturally immutable in java (primitives,
 *     their boxed equivalents and enums).  Likewise union types can only be created (via an
 *     overloaded constructor set) and cannot be mutated thereafter.
 *   </li>
 *   <li>
 *     <em>Thrift Type Annotation Support</em>
 *     <p>
 *     Any thrift annotations present in the underlying thrift files are transferred to the
 *     generated code via {@link ThriftAnnotation}.  Since {@code ThriftAnnotation} (and its nested
 *     {@link ThriftAnnotation.Parameter}) have runtime retention, the resulting thrift code can be
 *     further processed with annotation processors at compile time or via reflection at runtime.
 *   </li>
 * </ul>
 */
public final class ThriftGen {
  private final Path outdir;
  private final UnaryOperator<String> packageSuffixFactory;
  private final Logger logger;

  /**
   * Equivalent to {@link #ThriftGen(Path, Logger, UnaryOperator)} passig the identity operator for
   * the {@code packageSuffixFactory}.
   *
   * @param outdir A directory to emit generated code under.  Need not exist.
   * @param logger A logger to log code generation details to.
   */
  public ThriftGen(Path outdir, Logger logger) {
    this(outdir, logger, UnaryOperator.identity());
  }

  /**
   * Create a new generator that will output to the given {@code outdir}.
   *
   * @param outdir A directory to emit generated code under.  Need not exist.
   * @param logger A logger to log code generation details to.
   * @param packageSuffixFactory Given the package name derived from a thrift file, return the final
   *                             package name to be used for the code generated for that thrift
   *                             file.
   */
  public ThriftGen(Path outdir, Logger logger, UnaryOperator<String> packageSuffixFactory) {
    this.outdir = requireNonNull(outdir);
    this.logger = requireNonNull(logger);
    this.packageSuffixFactory = requireNonNull(packageSuffixFactory);
  }

  /**
   * Generates java code for the given set of closed thrift files.
   * <p>
   * NB: If any of the given thrift files have includes, they will be resolved from the same
   * directory tree as the including file.
   *
   * @param thriftFiles The root set of thrift files to process.
   * @return The set of files processed which may include includes not in the original set of
   *         {@code thriftFiles}.
   * @throws IOException If there are any problems emitting java code.
   */
  public ImmutableSet<Path> generate(ImmutableSet<Path> thriftFiles) throws IOException {
    Set<Path> processed = new HashSet<>();
    SymbolTable symbolTable = new SymbolTable();
    processThriftFiles(
        processed,
        symbolTable,
        Iterables.transform(thriftFiles, Path::toAbsolutePath),
        /* required */ false);
    return ImmutableSet.copyOf(processed);
  }

  private SymbolTable processThriftFiles(
      Set<Path> processed,
      SymbolTable symbolTable,
      Iterable<Path> thriftFiles,
      boolean required)
      throws IOException {

    for (Path thriftFile : thriftFiles) {
      if (!processed.contains(thriftFile)) {
        processed.add(thriftFile);
        symbolTable = processThriftFile(processed, symbolTable, required, thriftFile);
      }
    }
    return symbolTable;
  }

  static class PathCharSource extends CharSource {
    private final Path path;
    private final Charset charset;

    PathCharSource(Path path, Charset charset) {
      this.path = path;
      this.charset = charset;
    }

    @Override
    public Reader openStream() throws IOException {
      return Files.newBufferedReader(path, charset);
    }
  }

  private SymbolTable processThriftFile(
      Set<Path> processed,
      SymbolTable symbolTable,
      boolean required,
      Path thriftFile)
      throws IOException {

    CharSource thriftIdl = new PathCharSource(thriftFile, Charsets.UTF_8);
    Document document = ThriftIdlParser.parseThriftIdl(thriftIdl);
    String packageName = document.getHeader().getNamespace("java");
    if (packageName == null) {
      if (required) {
        throw new IllegalArgumentException(
            String.format("%s must declare a 'java' namespace", thriftFile));
      } else {
        logger.warn("Skipping {} - no java namespace", thriftFile);
      }
    } else {
      symbolTable = symbolTable.updated(thriftFile, packageName, document.getDefinitions());
      ImmutableSet<Path> includes =
          FluentIterable.from(document.getHeader().getIncludes())
              .transform(inc -> thriftFile.getParent().resolve(inc).toAbsolutePath())
              .filter(f -> !processed.contains(f))
              .toSet();
      symbolTable = processThriftFiles(processed, symbolTable, includes, /* required */ true);

      ThriftGenVisitor visitor =
          new ThriftGenVisitor(
              logger,
              outdir,
              symbolTable,
              packageSuffixFactory.apply(packageName));
      document.visit(visitor);
      visitor.finish();
      processed.add(thriftFile);
    }
    return symbolTable;
  }
}
