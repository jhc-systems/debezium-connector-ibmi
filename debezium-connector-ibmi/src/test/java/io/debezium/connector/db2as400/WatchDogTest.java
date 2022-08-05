/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import org.fest.assertions.Assertions;
import org.junit.Test;

public class WatchDogTest {

    private WatchDog createTestSubject() {
        return new WatchDog(Thread.currentThread(), 10);
    }

    @Test
    public void testRun() throws Exception {
        WatchDog testSubject = createTestSubject();
        testSubject.start();
        Exception thrown = null;
        try {
            Thread.sleep(200);
        }
        catch (Exception e) {
            thrown = e;
        }
        Assertions.assertThat(thrown).isInstanceOf(InterruptedException.class);
        testSubject.stop();
    }
}