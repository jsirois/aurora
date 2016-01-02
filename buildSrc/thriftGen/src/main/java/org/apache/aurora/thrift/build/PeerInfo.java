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

import com.facebook.swift.parser.model.AbstractStruct;
import com.facebook.swift.parser.model.TypeAnnotation;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

class PeerInfo {
  static Optional<PeerInfo> from(String packageName, AbstractStruct struct) {
    return FluentIterable.from(struct.getAnnotations())
        .filter(a -> "mutablePeer".equals(a.getName()))
        .transform(TypeAnnotation::getValue)
        .first()
        .transform(value -> new PeerInfo(packageName, struct, value));
  }

  final boolean render;
  final String packageName;
  final String className;

  private PeerInfo(String structPackageName, AbstractStruct struct, String mutablePeerValue) {
    render = Boolean.parseBoolean(mutablePeerValue);
    if (render) {
      packageName = structPackageName + ".peer";
      className = "Mutable" + struct.getName();
    } else {
      int i = mutablePeerValue.lastIndexOf('.');
      if (i == -1) {
        packageName = "";
        className = mutablePeerValue;
      } else {
        packageName = mutablePeerValue.substring(0, i);
        className = mutablePeerValue.substring(i + 1);
      }
    }
  }
}
