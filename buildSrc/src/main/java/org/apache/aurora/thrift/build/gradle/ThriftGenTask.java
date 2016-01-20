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
package org.apache.aurora.thrift.build.gradle;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.UnaryOperator;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.thrift.build.ThriftGen;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;

/**
 * Generates thrift stubs for structs and services.
 */
public class ThriftGenTask extends DefaultTask {
  @TaskAction
  public void gen() throws IOException {
    Path outdir = getOutputs().getFiles().getSingleFile().toPath();
    ThriftGen thriftGen = new ThriftGen(outdir, getLogger());
    ImmutableSet<Path> thriftFiles =
        FluentIterable.from(getInputs().getFiles().getFiles())
            .transform(File::toPath)
            .transform(Path::toAbsolutePath)
            .toSet();
    thriftGen.generate(thriftFiles);
  }
}
