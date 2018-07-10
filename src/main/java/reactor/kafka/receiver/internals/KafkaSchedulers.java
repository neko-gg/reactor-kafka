/*
 * Copyright (c) 2016-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.kafka.receiver.internals;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.scheduler.NonBlocking;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * The set of common factory methods using within Kafka Receiver
 */
class KafkaSchedulers {
    static final Logger log = Loggers.getLogger(Schedulers.class);

    static final void defaultUncaughtException(Thread t, Throwable e) {
        log.error("KafkaScheduler worker in group " + t.getThreadGroup().getName()
                + " failed with an uncaught exception", e);
    }

    static EventScheduler newEvent(String groupId) {
        return new EventScheduler(groupId);
    }

    static Scheduler fromWorker(Scheduler.Worker worker) {
        return new WorkerScheduler(worker);
    }

    final static class EventScheduler implements Scheduler {

        final ThreadLocal<Boolean> holder = ThreadLocal.withInitial(() -> false);
        final Scheduler inner;

        private EventScheduler(String groupId) {
            this.inner = Schedulers.newSingle(new EventThreadFactory(groupId));
        }

        @Override
        public Disposable schedule(Runnable task) {
            return inner.schedule(decorate(task));
        }

        @Override
        public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            return inner.schedule(decorate(task), delay, unit);
        }

        @Override
        public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            return inner.schedulePeriodically(decorate(task), initialDelay, period, unit);
        }

        @Override
        public long now(TimeUnit unit) {
            return inner.now(unit);
        }

        @Override
        public Worker createWorker() {
            return inner.createWorker();
        }

        @Override
        public void dispose() {
            inner.dispose();
        }

        @Override
        public boolean isDisposed() {
            return inner.isDisposed();
        }

        @Override
        public void start() {
            inner.start();
        }

        boolean isCurrentThreadFromScheduler() {
            return holder.get();
        }

        private Runnable decorate(Runnable task) {
            return () -> {
                holder.set(true);
                task.run();
            };
        }
    }

    final static class EventThreadFactory implements ThreadFactory {

        static final String     PREFIX            = "reactive-kafka-";
        static final AtomicLong COUNTER_REFERENCE = new AtomicLong();

        final private String groupId;

        EventThreadFactory(String groupId) {
            this.groupId = groupId;
        }

        @Override
        public final Thread newThread(Runnable runnable) {
            String newThreadName = PREFIX + groupId + "-" + COUNTER_REFERENCE.incrementAndGet();
            Thread t = new EmitterThread(runnable, newThreadName);
            t.setUncaughtExceptionHandler(KafkaSchedulers::defaultUncaughtException);
            return t;
        }

        static final class EmitterThread extends Thread implements NonBlocking {

            final ThreadLocal<Boolean> local = new ThreadLocal<>();

            EmitterThread(Runnable target, String name) {
                super(target, name);
            }
        }
    }

    final static class WorkerScheduler implements Scheduler {

        private final Worker worker;

        private WorkerScheduler(Worker worker) {
            this.worker = worker;
        }

        @Override
        public Disposable schedule(Runnable task) {
            return worker.schedule(task);
        }

        @Override
        public Disposable schedulePeriodically(Runnable task,
                long initialDelay,
                long period,
                TimeUnit unit) {
            return worker.schedulePeriodically(task, initialDelay, period, unit);
        }

        @Override
        public Worker createWorker() {
            return new WorkerDecorator(worker);
        }

        @Override
        public void dispose() {
            worker.dispose();
        }

        @Override
        public boolean isDisposed() {
            return worker.isDisposed();
        }

        final static class WorkerDecorator implements Worker {
            final Worker worker;
            final Composite tasks;

            WorkerDecorator(Worker worker) {
                this.worker = worker;
                this.tasks = Disposables.composite();

            }

            @Override
            public void dispose() {
                tasks.dispose();
            }

            @Override
            public boolean isDisposed() {
                return tasks.isDisposed();
            }

            @Override
            public Disposable schedule(Runnable task) {
                Disposable disposableTask = worker.schedule(task);
                tasks.add(disposableTask);
                return disposableTask;
            }

            @Override
            public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                Disposable disposableTask = worker.schedule(task, delay, unit);
                tasks.add(disposableTask);
                return disposableTask;
            }

            @Override
            public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
                Disposable disposableTask = worker.schedulePeriodically(task, initialDelay, period, unit);
                tasks.add(disposableTask);
                return disposableTask;
            }
        }
    }
}
