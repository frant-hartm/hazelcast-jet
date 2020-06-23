/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.elastic.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.Callable;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * Static utility class to retry operations related to connecting to AWS Services.
 */
public final class RetryUtils {
    static final long INITIAL_BACKOFF_MS = 2000L;
    static final long MAX_BACKOFF_MS = 5 * 60 * 1000L;
    static final double BACKOFF_MULTIPLIER = 2;

    private static final ILogger LOGGER = Logger.getLogger(RetryUtils.class);

    private static final long MS_IN_SECOND = 1000L;

    private RetryUtils() {
    }

    /**
     * Calls {@code callable.call()} until it does not throw an exception (but no more than {@code retries} times).
     * <p>
     * If {@code callable} throws an unchecked exception, it is wrapped into {@link HazelcastException}.
     */
    @SafeVarargs
    public static <T> T retry(Callable<T> callable, int retries, Class<? extends Exception> ... exceptions) {
        int retryCount = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                if (anyOf(e, exceptions)) {
                    retryCount++;
                    if (retryCount > retries) {
                        throw sneakyThrow(e);
                    }
                    long waitIntervalMs = backoffIntervalForRetry(retryCount);
                    LOGGER.fine(String.format("Couldn't connect to Elastic, [%s] retrying in %s seconds...", retryCount,
                            waitIntervalMs / MS_IN_SECOND));
                    sleep(waitIntervalMs);
                } else {
                    throw sneakyThrow(e);
                }
            }
        }
    }

    private static boolean anyOf(Exception e, Class<? extends Exception>[] exceptions) {
        for (Class<? extends Exception> exception : exceptions) {
            if (exception.isAssignableFrom(e.getClass())) {
                return true;
            }
        }
        return false;
    }

    private static long backoffIntervalForRetry(int retryCount) {
        long result = INITIAL_BACKOFF_MS;
        for (int i = 1; i < retryCount; i++) {
            result *= BACKOFF_MULTIPLIER;
            if (result > MAX_BACKOFF_MS) {
                return MAX_BACKOFF_MS;
            }
        }
        return result;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HazelcastException(e);
        }
    }
}
