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
package org.apache.aurora.scheduler.storage;

import java.util.Optional;
import java.util.Set;

import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;

/**
 * Stores all lock-related data and defines methods for saving, deleting and fetching locks.
 */
public interface LockStore {
  /**
   * Fetches all locks available in the store.
   *
   * @return All locks in the store.
   */
  Set<Lock> fetchLocks();

  /**
   * Fetches a lock by its key.
   *
   * @param lockKey Key of the lock to fetch.
   * @return Optional lock.
   */
  Optional<Lock> fetchLock(LockKey lockKey);

  interface Mutable extends LockStore {
    /**
     * Saves a new lock or overwrites the existing one with same LockKey.
     *
     * @param lock Lock to save.
     */
    void saveLock(Lock lock);

    /**
     * Removes the lock from the store.
     *
     * @param lockKey Key of the lock to remove.
     */
    void removeLock(LockKey lockKey);

    /**
     * Deletes all locks from the store.
     */
    void deleteLocks();
  }
}
