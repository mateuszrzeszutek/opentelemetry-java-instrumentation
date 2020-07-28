/*
 * Copyright The OpenTelemetry Authors
 *
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

import com.google.common.util.concurrent.MoreExecutors
import io.opentelemetry.auto.instrumentation.spymemcached.CompletionListener
import io.opentelemetry.auto.test.AgentTestRunner
import io.opentelemetry.auto.test.asserts.TraceAssert
import io.opentelemetry.trace.attributes.StringAttributeSetter
import net.spy.memcached.CASResponse
import net.spy.memcached.ConnectionFactory
import net.spy.memcached.ConnectionFactoryBuilder
import net.spy.memcached.DefaultConnectionFactory
import net.spy.memcached.MemcachedClient
import net.spy.memcached.internal.CheckedOperationTimeoutException
import net.spy.memcached.ops.Operation
import net.spy.memcached.ops.OperationQueueFactory
import org.testcontainers.containers.GenericContainer
import spock.lang.Requires
import spock.lang.Shared

import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.locks.ReentrantLock

import static io.opentelemetry.auto.test.utils.TraceUtils.runUnderTrace
import static io.opentelemetry.trace.Span.Kind.CLIENT
import static net.spy.memcached.ConnectionFactoryBuilder.Protocol.BINARY

// Do not run tests on Java7 since testcontainers are not compatible with Java7
@Requires({ jvm.java8Compatible })
class SpymemcachedTest extends AgentTestRunner {

  @Shared
  def parentOperation = "parent-span"
  @Shared
  def expiration = 3600
  @Shared
  def keyPrefix = "SpymemcachedTest-" + (Math.abs(new Random().nextInt())) + "-"
  @Shared
  def defaultMemcachedPort = 11211
  @Shared
  def timingOutMemcachedOpTimeout = 1000

  /*
    Note: type here has to stay undefined, otherwise tests will fail in CI in Java 7 because
    'testcontainers' are built for Java 8 and Java 7 cannot load this class.
   */
  @Shared
  def memcachedContainer
  @Shared
  InetSocketAddress memcachedAddress = new InetSocketAddress("127.0.0.1", defaultMemcachedPort)

  def setupSpec() {

    /*
      CircleCI will provide us with memcached container running along side our build.
      When building locally and in GitHub actions, however, we need to take matters into our own hands
      and we use 'testcontainers' for this.
     */
    if ("true" != System.getenv("CIRCLECI")) {
      memcachedContainer = new GenericContainer('memcached:latest')
        .withExposedPorts(defaultMemcachedPort)
        .withStartupTimeout(Duration.ofSeconds(120))
      memcachedContainer.start()
      memcachedAddress = new InetSocketAddress(
        memcachedContainer.containerIpAddress,
        memcachedContainer.getMappedPort(defaultMemcachedPort)
      )
    }
  }

  def cleanupSpec() {
    if (memcachedContainer) {
      memcachedContainer.stop()
    }
  }

  ReentrantLock queueLock
  MemcachedClient memcached
  MemcachedClient locableMemcached
  MemcachedClient timingoutMemcached

  def setup() {
    queueLock = new ReentrantLock()

    // Use direct executor service so our listeners finish in deterministic order
    ExecutorService listenerExecutorService = MoreExecutors.newDirectExecutorService()

    ConnectionFactory connectionFactory = (new ConnectionFactoryBuilder())
      .setListenerExecutorService(listenerExecutorService)
      .setProtocol(BINARY)
      .build()
    memcached = new MemcachedClient(connectionFactory, Arrays.asList(memcachedAddress))

    def lockableQueueFactory = new OperationQueueFactory() {
      @Override
      BlockingQueue<Operation> create() {
        return getLockableQueue(queueLock)
      }
    }

    ConnectionFactory lockableConnectionFactory = (new ConnectionFactoryBuilder())
      .setListenerExecutorService(listenerExecutorService)
      .setProtocol(BINARY)
      .setOpQueueFactory(lockableQueueFactory)
      .build()
    locableMemcached = new MemcachedClient(lockableConnectionFactory, Arrays.asList(memcachedAddress))

    ConnectionFactory timingoutConnectionFactory = (new ConnectionFactoryBuilder())
      .setListenerExecutorService(listenerExecutorService)
      .setProtocol(BINARY)
      .setOpQueueFactory(lockableQueueFactory)
      .setOpTimeout(timingOutMemcachedOpTimeout)
      .build()
    timingoutMemcached = new MemcachedClient(timingoutConnectionFactory, Arrays.asList(memcachedAddress))

    // Add some keys to test on later:
    def valuesToSet = [
      "test-get"    : "get test",
      "test-get-2"  : "get test 2",
      "test-append" : "append test",
      "test-prepend": "prepend test",
      "test-delete" : "delete test",
      "test-replace": "replace test",
      "test-touch"  : "touch test",
      "test-cas"    : "cas test",
      "test-decr"   : "200",
      "test-incr"   : "100"
    ]
    runUnderTrace("setup") {
      valuesToSet.each { k, v -> assert memcached.set(key(k), expiration, v).get() }
    }
    TEST_WRITER.waitForTraces(1)
    TEST_WRITER.clear()
  }

  def "test get hit"() {
    when:
    runUnderTrace(parentOperation) {
      assert "get test" == memcached.get(key("test-get"))
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "get", null, "hit")
      }
    }
  }

  def "test get miss"() {
    when:
    runUnderTrace(parentOperation) {
      assert null == memcached.get(key("test-get-key-that-doesn't-exist"))
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "get", null, "miss")
      }
    }
  }

  def "test get cancel"() {
    when:
    runUnderTrace(parentOperation) {
      queueLock.lock()
      locableMemcached.asyncGet(key("test-get")).cancel(true)
      queueLock.unlock()
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "get", "canceled")
      }
    }
  }

  def "test get timeout"() {
    when:
    /*
     Not using runUnderTrace since timeouts happen in separate thread
     and direct executor doesn't help to make sure that parent span finishes last.
     Instead run without parent span to have only 1 span to test with.
      */
    try {
      queueLock.lock()
      timingoutMemcached.asyncGet(key("test-get"))
      Thread.sleep(timingOutMemcachedOpTimeout + 1000)
    } finally {
      queueLock.unlock()
    }

    then:
    assertTraces(1) {
      trace(0, 1) {
        getSpan(it, 0, "get", "timeout")
      }
    }
  }

  def "test bulk get"() {
    when:
    runUnderTrace(parentOperation) {
      def expected = [(key("test-get")): "get test", (key("test-get-2")): "get test 2"]
      assert expected == memcached.getBulk(key("test-get"), key("test-get-2"))
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "getBulk", null, null)
      }
    }
  }

  def "test set"() {
    when:
    runUnderTrace(parentOperation) {
      assert memcached.set(key("test-set"), expiration, "bar").get()
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "set")
      }
    }
  }

  def "test set cancel"() {
    when:
    runUnderTrace(parentOperation) {
      queueLock.lock()
      assert locableMemcached.set(key("test-set-cancel"), expiration, "bar").cancel()
      queueLock.unlock()
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "set", "canceled")
      }
    }
  }

  def "test add"() {
    when:
    runUnderTrace(parentOperation) {
      assert memcached.add(key("test-add"), expiration, "add bar").get()
      assert "add bar" == memcached.get(key("test-add"))
    }

    then:
    assertTraces(1) {
      trace(0, 3) {
        getParentSpan(it, 0)
        getSpan(it, 1, "add")
        getSpan(it, 2, "get", null, "hit")
      }
    }
  }

  def "test second add"() {
    when:
    runUnderTrace(parentOperation) {
      assert memcached.add(key("test-add-2"), expiration, "add bar").get()
      assert !memcached.add(key("test-add-2"), expiration, "add bar 123").get()
    }

    then:
    assertTraces(1) {
      trace(0, 3) {
        getParentSpan(it, 0)
        getSpan(it, 1, "add")
        getSpan(it, 2, "add")
      }
    }
  }

  def "test delete"() {
    when:
    runUnderTrace(parentOperation) {
      assert memcached.delete(key("test-delete")).get()
      assert null == memcached.get(key("test-delete"))
    }

    then:
    assertTraces(1) {
      trace(0, 3) {
        getParentSpan(it, 0)
        getSpan(it, 1, "delete")
        getSpan(it, 2, "get", null, "miss")
      }
    }
  }

  def "test delete non existent"() {
    when:
    runUnderTrace(parentOperation) {
      assert !memcached.delete(key("test-delete-non-existent")).get()
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "delete")
      }
    }
  }

  def "test replace"() {
    when:
    runUnderTrace(parentOperation) {
      assert memcached.replace(key("test-replace"), expiration, "new value").get()
      assert "new value" == memcached.get(key("test-replace"))
    }

    then:
    assertTraces(1) {
      trace(0, 3) {
        getParentSpan(it, 0)
        getSpan(it, 1, "replace")
        getSpan(it, 2, "get", null, "hit")
      }
    }
  }

  def "test replace non existent"() {
    when:
    runUnderTrace(parentOperation) {
      assert !memcached.replace(key("test-replace-non-existent"), expiration, "new value").get()
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "replace")
      }
    }
  }

  def "test append"() {
    when:
    runUnderTrace(parentOperation) {
      def cas = memcached.gets(key("test-append"))
      assert memcached.append(cas.cas, key("test-append"), " appended").get()
      assert "append test appended" == memcached.get(key("test-append"))
    }

    then:
    assertTraces(1) {
      trace(0, 4) {
        getParentSpan(it, 0)
        getSpan(it, 1, "gets")
        getSpan(it, 2, "append")
        getSpan(it, 3, "get", null, "hit")
      }
    }
  }

  def "test prepend"() {
    when:
    runUnderTrace(parentOperation) {
      def cas = memcached.gets(key("test-prepend"))
      assert memcached.prepend(cas.cas, key("test-prepend"), "prepended ").get()
      assert "prepended prepend test" == memcached.get(key("test-prepend"))
    }

    then:
    assertTraces(1) {
      trace(0, 4) {
        getParentSpan(it, 0)
        getSpan(it, 1, "gets")
        getSpan(it, 2, "prepend")
        getSpan(it, 3, "get", null, "hit")
      }
    }
  }

  def "test cas"() {
    when:
    runUnderTrace(parentOperation) {
      def cas = memcached.gets(key("test-cas"))
      assert CASResponse.OK == memcached.cas(key("test-cas"), cas.cas, expiration, "cas bar")
    }

    then:
    assertTraces(1) {
      trace(0, 3) {
        getParentSpan(it, 0)
        getSpan(it, 1, "gets")
        getSpan(it, 2, "cas")
      }
    }
  }

  def "test cas not found"() {
    when:
    runUnderTrace(parentOperation) {
      assert CASResponse.NOT_FOUND == memcached.cas(key("test-cas-doesnt-exist"), 1234, expiration, "cas bar")
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "cas")
      }
    }
  }

  def "test touch"() {
    when:
    runUnderTrace(parentOperation) {
      assert memcached.touch(key("test-touch"), expiration).get()
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "touch")
      }
    }
  }

  def "test touch non existent"() {
    when:
    runUnderTrace(parentOperation) {
      assert !memcached.touch(key("test-touch-non-existent"), expiration).get()
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "touch")
      }
    }
  }

  def "test get and touch"() {
    when:
    runUnderTrace(parentOperation) {
      assert "touch test" == memcached.getAndTouch(key("test-touch"), expiration).value
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "getAndTouch")
      }
    }
  }

  def "test get and touch non existent"() {
    when:
    runUnderTrace(parentOperation) {
      assert null == memcached.getAndTouch(key("test-touch-non-existent"), expiration)
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "getAndTouch")
      }
    }
  }

  def "test decr"() {
    when:
    runUnderTrace(parentOperation) {
      /*
        Memcached is funny in the way it handles incr/decr operations:
        it needs values to be strings (with digits in them) and it returns actual long from decr/incr
       */
      assert 195 == memcached.decr(key("test-decr"), 5)
      assert "195" == memcached.get(key("test-decr"))
    }

    then:
    assertTraces(1) {
      trace(0, 3) {
        getParentSpan(it, 0)
        getSpan(it, 1, "decr")
        getSpan(it, 2, "get", null, "hit")
      }
    }
  }

  def "test decr non existent"() {
    when:
    runUnderTrace(parentOperation) {
      assert -1 == memcached.decr(key("test-decr-non-existent"), 5)
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "decr")
      }
    }
  }

  def "test decr exception"() {
    when:
    memcached.decr(key("long key: " + longString()), 5)

    then:
    thrown IllegalArgumentException
    assertTraces(1) {
      trace(0, 1) {
        getSpan(it, 0, "decr", "long key")
      }
    }
  }

  def "test incr"() {
    when:
    runUnderTrace(parentOperation) {
      /*
        Memcached is funny in the way it handles incr/decr operations:
        it needs values to be strings (with digits in them) and it returns actual long from decr/incr
       */
      assert 105 == memcached.incr(key("test-incr"), 5)
      assert "105" == memcached.get(key("test-incr"))
    }

    then:
    assertTraces(1) {
      trace(0, 3) {
        getParentSpan(it, 0)
        getSpan(it, 1, "incr")
        getSpan(it, 2, "get", null, "hit")
      }
    }
  }

  def "test incr non existent"() {
    when:
    runUnderTrace(parentOperation) {
      assert -1 == memcached.incr(key("test-incr-non-existent"), 5)
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        getParentSpan(it, 0)
        getSpan(it, 1, "incr")
      }
    }
  }

  def "test incr exception"() {
    when:
    memcached.incr(key("long key: " + longString()), 5)

    then:
    thrown IllegalArgumentException
    assertTraces(1) {
      trace(0, 1) {
        getSpan(it, 0, "incr", "long key")
      }
    }
  }

  def key(String k) {
    keyPrefix + k
  }

  def longString(char c = 's' as char) {
    char[] chars = new char[250]
    Arrays.fill(chars, 's' as char)
    return new String(chars)
  }

  def getLockableQueue(ReentrantLock queueLock) {
    return new ArrayBlockingQueue<Operation>(DefaultConnectionFactory.DEFAULT_OP_QUEUE_LEN) {

      @Override
      int drainTo(Collection<? super Operation> c, int maxElements) {
        try {
          queueLock.lock()
          return super.drainTo(c, maxElements)
        } finally {
          queueLock.unlock()
        }
      }
    }
  }

  def getParentSpan(TraceAssert trace, int index) {
    return trace.span(index) {
      operationName parentOperation
      parent()
      errored false
      attributes {
      }
    }
  }

  def getSpan(TraceAssert trace, int index, String operation, String error = null, String result = null) {
    return trace.span(index) {
      if (index > 0) {
        childOf(trace.span(0))
      }

      operationName operation
      spanKind CLIENT
      errored(error != null && error != "canceled")

      if (error == "timeout") {
        errorEvent(
          CheckedOperationTimeoutException,
          "Operation timed out. - failing node: ${memcachedAddress.address}:${memcachedAddress.port}")
      }

      if (error == "long key") {
        errorEvent(
          IllegalArgumentException,
          "Key is too long (maxlen = 250)")
      }

      attributes {
        "${StringAttributeSetter.create("db.system").key()}" "memcached"

        if (error == "canceled") {
          "${CompletionListener.DB_COMMAND_CANCELLED}" true
        }

        if (result == "hit") {
          "${CompletionListener.MEMCACHED_RESULT}" CompletionListener.HIT
        }

        if (result == "miss") {
          "${CompletionListener.MEMCACHED_RESULT}" CompletionListener.MISS
        }
      }
    }
  }
}
