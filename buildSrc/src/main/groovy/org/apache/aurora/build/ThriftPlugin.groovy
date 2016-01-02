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
package org.apache.aurora.build

import org.apache.aurora.thrift.build.gradle.ThriftGenTask
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.compile.JavaCompile

class ThriftPlugin implements Plugin<Project> {
  @Override
  void apply(Project project) {
    project.configure(project) {
      apply plugin: 'java'

      extensions.create('thrift', ThriftPluginExtension, project)

      configurations.create('thriftCompile')
      afterEvaluate {
        dependencies {
          thriftCompile project.project(':buildSrc:thriftGen')
          thriftCompile "org.apache.thrift:libthrift:${thrift.version}"
          // TODO(John Sirois): Scope the new gen as seperate from the old thrift c gen.
          // TODO(John Sirois): Use the ${thrift.version} pattern to get the versions below from the
          // consuming project(s).
          thriftCompile "com.facebook.swift:swift-annotations:0.16.0"
          thriftCompile "com.facebook.swift:swift-codec:0.16.0"
          thriftCompile "com.google.auto.value:auto-value:1.1"
          thriftCompile "com.google.guava:guava:18.0"
          thriftCompile "org.immutables:value:2.1.0"
        }
      }

      task(type: ThriftGenTask, 'generateThriftJava') {
        inputs.files {
          thrift.inputFiles
        }
        outputs.dir {
          thrift.genRestDir
        }
      }

      task('generateThriftResources') {
        inputs.files {
          thrift.inputFiles
        }
        outputs.dir {
          thrift.genResourcesDir
        }
        doLast {
          def dest = file("${thrift.genResourcesDir}/${thrift.resourcePrefix}")
          dest.exists() || dest.mkdirs()
          thrift.inputFiles.each { File file ->
            exec {
              commandLine thrift.wrapperPath, thrift.version,
                  '--gen', 'js:jquery',
                  '--gen', 'html:standalone',
                  '-out', dest.path, file.path
            }
          }
        }
      }

      task('classesThrift', type: JavaCompile) {
        afterEvaluate {
          source = files(generateThriftJava)
          if (thrift.peerSources) {
            source += files(thrift.peerSources)
          }
          classpath = configurations.thriftCompile
          destinationDir = file(thrift.genClassesDir)
          options.warnings = false
          // Capture method parameter names in classfiles.
          options.compilerArgs << '-parameters'
        }
      }

      configurations.create('thriftRuntime')
      configurations.thriftRuntime.extendsFrom(configurations.thriftCompile)
      configurations.compile.extendsFrom(configurations.thriftRuntime)

      def peerClasses = file("${projectDir}/dist/classes/main")
      peerClasses.exists() || peerClasses.mkdirs()

      dependencies {
        thriftRuntime files(classesThrift)
        thriftRuntime files(peerClasses)
      }

      sourceSets.main {
        output.dir(classesThrift)
        output.dir(peerClasses)
        output.dir(generateThriftResources)
      }
    }
  }
}

class ThriftPluginExtension {
  def wrapperPath
  File genResourcesDir
  File genJavaDir
  File genRestDir
  File genClassesDir
  FileTree inputFiles

  String version
  String getVersion() {
    if (version == null) {
      throw new GradleException('thrift.version is required.')
    } else {
      return version
    }
  }

  String resourcePrefix

  /* Classpath prefix for generated resources. */
  String getResourcesPrefix() {
    if (resourcePrefix == null) {
      throw new GradleException('thrift.resourcePrefix is required.')
    } else {
      return resourcePrefix
    }
  }

  /* Optional source set that contains hand-coded peers. */
  File peerSources

  ThriftPluginExtension(Project project) {
    wrapperPath = "${project.rootDir}/build-support/thrift/thriftw"
    genResourcesDir = project.file("${project.buildDir}/thrift/gen-resources")
    genJavaDir = project.file("${project.buildDir}/thrift/gen-java")
    genRestDir = project.file("${project.buildDir}/thrift/gen-rest")
    genClassesDir = project.file("${project.buildDir}/thrift/classes")
    inputFiles = project.fileTree("src/main/thrift").matching {
      include "**/*.thrift"
    }
  }
}
