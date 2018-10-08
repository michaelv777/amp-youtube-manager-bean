package amp.source.youtube.worker;



/*
2  * Licensed to the Apache Software Foundation (ASF) under one or more
3  * contributor license agreements.  See the NOTICE file distributed with
4  * this work for additional information regarding copyright ownership.
5  * The ASF licenses this file to You under the Apache License, Version 2.0
6  * (the "License"); you may not use this file except in compliance with
7  * the License.  You may obtain a copy of the License at
8  *
9  *     http://www.apache.org/licenses/LICENSE-2.0
10 *
11 * Unless required by applicable law or agreed to in writing, software
12 * distributed under the License is distributed on an "AS IS" BASIS,
13 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
14 * See the License for the specific language governing permissions and
15 * limitations under the License.
16 */


import javax.enterprise.concurrent.ManageableThread;
import javax.enterprise.concurrent.ManagedThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ManagedThreadFactoryImpl implements ManagedThreadFactory {
    private static final AtomicInteger ID = new AtomicInteger();

    private final String prefix;

    public ManagedThreadFactoryImpl() {
        this.prefix = "managed-thread-";
    }

    public ManagedThreadFactoryImpl(final String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(final Runnable r) {
        final Thread thread = new ManagedThread(r);
        thread.setDaemon(true);
        thread.setName(prefix + ID.incrementAndGet());
        thread.setContextClassLoader(ManagedThreadFactoryImpl.class.getClassLoader()); // ensure we use container loader as main context classloader to avoid leaks
        return thread;
    }

    public static class ManagedThread extends Thread implements ManageableThread {
        public ManagedThread(final Runnable r) {
            super(r);
        }

        @Override
        public boolean isShutdown() {
            return !isAlive();
        }
    }
}
