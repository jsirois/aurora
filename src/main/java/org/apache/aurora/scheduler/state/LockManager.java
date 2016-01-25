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
package org.apache.aurora.scheduler.state;

import java.util.Optional;

import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;

/**
 * Defines all {@link Lock} primitives like: acquire, release, validate.
 */
public interface LockManager {
  /**
   * Creates, saves and returns a new {@link Lock} with the specified {@link LockKey}.
   * This method is not re-entrant, i.e. attempting to acquire a lock with the
   * same key would throw a {@link LockException}.
   *
   * @param lockKey A key uniquely identify the lock to be created.
   * @param user Name of the user requesting a lock.
   * @return A new Lock instance.
   * @throws LockException In case the lock with specified key already exists.
   */
  Lock acquireLock(LockKey lockKey, String user) throws LockException;

  /**
   * Releases (removes) the specified {@link Lock} from the system.
   *
   * @param lock {@link Lock} to remove from the system.
   */
  void releaseLock(Lock lock);

  /**
   * Verifies if the provided lock instance is identical to the one stored in the scheduler
   * ONLY if the operation context represented by the {@link LockKey} is in fact locked.
   * No validation will be performed in case there is no correspondent scheduler lock
   * found for the provided context.
   *
   * @param context Operation context to validate with the provided lock.
   * @param heldLock Lock to validate.
   * @throws LockException If provided lock does not exist or not identical to the stored one.
   */
  void validateIfLocked(LockKey context, Optional<Lock> heldLock) throws LockException;

  /**
   * Returns all available locks stored.
   *
   * @return Set of {@link Lock} instances.
   */
  Iterable<Lock> getLocks();

  /**
   * Thrown when {@link Lock} related operation failed.
   */
  class LockException extends Exception {
    public LockException(String msg) {
      super(msg);
    }
  }
}
