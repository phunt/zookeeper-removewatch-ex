/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.phunt.zookeeper.removewatchexample;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
 * Very simple test class which attempts to exercise the removeWatches feature added in 3.5.0
 *
 */
public class ExerciseRemoveWatchTest implements Watcher {
	private static final boolean USE_REMOVE_CALL = true;
	
	private ZooKeeper zk; 
	private String rootZNode;
	private CountDownLatch zkAvail;
	
	@Before
	public void setUp() throws Exception {
		zkAvail = new CountDownLatch(1);
		zk = new ZooKeeper("localhost:2181", 30, this);
		zkAvail.await(30, TimeUnit.SECONDS);
		
		rootZNode = zk.create("/", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
	}

	@After
	public void tearDown() throws Exception {
		zk.close();
	}

	/**
	 * Exercise the functionality of remove watches added in ZooKeeper 3.5.0
	 * 
	 * Print some basic mem details to make sure we're in the ballpark
	 */
	@Test
	public void createAndRemoveLotsOfWatches() throws Exception {
		final int maxIter = 100000;
		final int maxLoops = 10000;
		System.out.println("USE_REMOVE_CALL:" + USE_REMOVE_CALL);
		System.out.println("maxIter:" + maxIter);
		System.out.println("maxLoops:" + maxLoops);
		Runtime rt = Runtime.getRuntime();
		rt.gc();
		rt.gc();
		rt.gc();
		System.out.println("Available free mem is " + rt.freeMemory());
		for (int j = 0; j < maxIter; j++) {
			System.out.println("j:" + j);
			IgnoringWatcher watcher = new IgnoringWatcher();
			for(int i = 0; i < maxLoops; i++) {
				String path = String.format("%s/e-%010d-%010d", rootZNode, j, i);
				zk.exists(path, watcher);
				if (USE_REMOVE_CALL) {
					zk.removeWatches(path, watcher, WatcherType.Any, true);
				}
			}
			rt.gc();
			rt.gc();
			rt.gc();
			System.out.println("Available free mem is " + rt.freeMemory());
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			zkAvail.countDown();
		}
	}

	private static class IgnoringWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
		}
	}
}
