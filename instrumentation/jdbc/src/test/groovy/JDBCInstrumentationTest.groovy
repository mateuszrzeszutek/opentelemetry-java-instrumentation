/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import static io.opentelemetry.auto.test.utils.TraceUtils.basicSpan
import static io.opentelemetry.auto.test.utils.TraceUtils.runUnderTrace
import static io.opentelemetry.trace.Span.Kind.CLIENT

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.opentelemetry.auto.test.AgentTestRunner
import io.opentelemetry.auto.test.utils.ConfigUtils
import io.opentelemetry.instrumentation.auto.jdbc.JDBCUtils
import io.opentelemetry.trace.attributes.SemanticAttributes
import java.sql.CallableStatement
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import javax.sql.DataSource
import org.apache.derby.jdbc.EmbeddedDataSource
import org.apache.derby.jdbc.EmbeddedDriver
import org.h2.Driver
import org.h2.jdbcx.JdbcDataSource
import org.hsqldb.jdbc.JDBCDriver
import spock.lang.Shared
import spock.lang.Unroll
import test.TestConnection

class JDBCInstrumentationTest extends AgentTestRunner {
  static final PREVIOUS_CONFIG = ConfigUtils.updateConfigAndResetInstrumentation {
    it.setProperty("otel.integration.jdbc-datasource.enabled", "true")
  }

  def specCleanup() {
    ConfigUtils.setConfig(PREVIOUS_CONFIG)
  }

  @Shared
  def dbName = "jdbcUnitTest"

  @Shared
  private Map<String, String> jdbcUrls = [
    "h2"    : "jdbc:h2:mem:$dbName",
    "derby" : "jdbc:derby:memory:$dbName",
    "hsqldb": "jdbc:hsqldb:mem:$dbName",
  ]

  @Shared
  private Map<String, String> jdbcDriverClassNames = [
    "h2"    : "org.h2.Driver",
    "derby" : "org.apache.derby.jdbc.EmbeddedDriver",
    "hsqldb": "org.hsqldb.jdbc.JDBCDriver",
  ]

  @Shared
  private Map<String, String> jdbcUserNames = [
    "h2"    : null,
    "derby" : "APP",
    "hsqldb": "SA",
  ]

  @Shared
  private Properties connectionProps = {
    def props = new Properties()
//    props.put("user", "someUser")
//    props.put("password", "somePassword")
    props.put("databaseName", "someDb")
    props.put("OPEN_NEW", "true") // So H2 doesn't complain about username/password.
    return props
  }()

  // JDBC Connection pool name (i.e. HikariCP) -> Map<dbName, Datasource>
  @Shared
  private Map<String, Map<String, DataSource>> cpDatasources = new HashMap<>()

  def prepareConnectionPoolDatasources() {
    String[] connectionPoolNames = [
      "tomcat", "hikari", "c3p0",
    ]
    connectionPoolNames.each {
      cpName ->
        Map<String, DataSource> dbDSMapping = new HashMap<>()
        jdbcUrls.each {
          dbType, jdbcUrl ->
            dbDSMapping.put(dbType, createDS(cpName, dbType, jdbcUrl))
        }
        cpDatasources.put(cpName, dbDSMapping)
    }
  }

  def createTomcatDS(String dbType, String jdbcUrl) {
    DataSource ds = new org.apache.tomcat.jdbc.pool.DataSource()
    def jdbcUrlToSet = dbType == "derby" ? jdbcUrl + ";create=true" : jdbcUrl
    ds.setUrl(jdbcUrlToSet)
    ds.setDriverClassName(jdbcDriverClassNames.get(dbType))
    String username = jdbcUserNames.get(dbType)
    if (username != null) {
      ds.setUsername(username)
    }
    ds.setPassword("")
    ds.setMaxActive(1) // to test proper caching, having > 1 max active connection will be hard to
    // determine whether the connection is properly cached
    return ds
  }

  def createHikariDS(String dbType, String jdbcUrl) {
    HikariConfig config = new HikariConfig()
    def jdbcUrlToSet = dbType == "derby" ? jdbcUrl + ";create=true" : jdbcUrl
    config.setJdbcUrl(jdbcUrlToSet)
    String username = jdbcUserNames.get(dbType)
    if (username != null) {
      config.setUsername(username)
    }
    config.setPassword("")
    config.addDataSourceProperty("cachePrepStmts", "true")
    config.addDataSourceProperty("prepStmtCacheSize", "250")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    config.setMaximumPoolSize(1)

    return new HikariDataSource(config)
  }

  def createC3P0DS(String dbType, String jdbcUrl) {
    DataSource ds = new ComboPooledDataSource()
    ds.setDriverClass(jdbcDriverClassNames.get(dbType))
    def jdbcUrlToSet = dbType == "derby" ? jdbcUrl + ";create=true" : jdbcUrl
    ds.setJdbcUrl(jdbcUrlToSet)
    String username = jdbcUserNames.get(dbType)
    if (username != null) {
      ds.setUser(username)
    }
    ds.setPassword("")
    ds.setMaxPoolSize(1)
    return ds
  }

  def createDS(String connectionPoolName, String dbType, String jdbcUrl) {
    DataSource ds = null
    if (connectionPoolName == "tomcat") {
      ds = createTomcatDS(dbType, jdbcUrl)
    }
    if (connectionPoolName == "hikari") {
      ds = createHikariDS(dbType, jdbcUrl)
    }
    if (connectionPoolName == "c3p0") {
      ds = createC3P0DS(dbType, jdbcUrl)
    }
    return ds
  }

  def setupSpec() {
    prepareConnectionPoolDatasources()
  }

  def cleanupSpec() {
    cpDatasources.values().each {
      it.values().each {
        datasource ->
          if (datasource instanceof Closeable) {
            datasource.close()
          }
      }
    }
  }

  @Unroll
  def "basic statement with #connection.getClass().getCanonicalName() on #driver generates spans"() {
    setup:
    Statement statement = connection.createStatement()
    ResultSet resultSet = runUnderTrace("parent") {
      return statement.executeQuery(query)
    }

    expect:
    resultSet.next()
    resultSet.getInt(1) == 3
    assertTraces(1) {
      trace(0, 2) {
        basicSpan(it, 0, "parent")
        span(1) {
          name JDBCUtils.normalizeSql(query)
          kind CLIENT
          childOf span(0)
          errored false
          attributes {
            "${SemanticAttributes.DB_SYSTEM.key()}" system
            "${SemanticAttributes.DB_NAME.key()}" dbName.toLowerCase()
            if (username != null) {
              "${SemanticAttributes.DB_USER.key()}" username
            }
            "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
            "${SemanticAttributes.DB_CONNECTION_STRING.key()}" url
          }
        }
      }
    }

    cleanup:
    statement.close()
    connection.close()

    where:
    system   | connection                                                           | username | query                                           | url
    "h2"     | new Driver().connect(jdbcUrls.get("h2"), null)                       | null     | "SELECT 3"                                      | "h2:mem:"
    "derby"  | new EmbeddedDriver().connect(jdbcUrls.get("derby"), null)            | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1"                | "derby:memory:"
    "hsqldb" | new JDBCDriver().connect(jdbcUrls.get("hsqldb"), null)               | "SA"     | "SELECT 3 FROM INFORMATION_SCHEMA.SYSTEM_USERS" | "hsqldb:mem:"
    "h2"     | new Driver().connect(jdbcUrls.get("h2"), connectionProps)            | null     | "SELECT 3"                                      | "h2:mem:"
    "derby"  | new EmbeddedDriver().connect(jdbcUrls.get("derby"), connectionProps) | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1"                | "derby:memory:"
    "hsqldb" | new JDBCDriver().connect(jdbcUrls.get("hsqldb"), connectionProps)    | "SA"     | "SELECT 3 FROM INFORMATION_SCHEMA.SYSTEM_USERS" | "hsqldb:mem:"
    "h2"     | cpDatasources.get("tomcat").get("h2").getConnection()                | null     | "SELECT 3"                                      | "h2:mem:"
    "derby"  | cpDatasources.get("tomcat").get("derby").getConnection()             | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1"                | "derby:memory:"
    "hsqldb" | cpDatasources.get("tomcat").get("hsqldb").getConnection()            | "SA"     | "SELECT 3 FROM INFORMATION_SCHEMA.SYSTEM_USERS" | "hsqldb:mem:"
    "h2"     | cpDatasources.get("hikari").get("h2").getConnection()                | null     | "SELECT 3"                                      | "h2:mem:"
    "derby"  | cpDatasources.get("hikari").get("derby").getConnection()             | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1"                | "derby:memory:"
    "hsqldb" | cpDatasources.get("hikari").get("hsqldb").getConnection()            | "SA"     | "SELECT 3 FROM INFORMATION_SCHEMA.SYSTEM_USERS" | "hsqldb:mem:"
    "h2"     | cpDatasources.get("c3p0").get("h2").getConnection()                  | null     | "SELECT 3"                                      | "h2:mem:"
    "derby"  | cpDatasources.get("c3p0").get("derby").getConnection()               | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1"                | "derby:memory:"
    "hsqldb" | cpDatasources.get("c3p0").get("hsqldb").getConnection()              | "SA"     | "SELECT 3 FROM INFORMATION_SCHEMA.SYSTEM_USERS" | "hsqldb:mem:"
  }

  @Unroll
  def "prepared statement execute on #driver with #connection.getClass().getCanonicalName() generates a span"() {
    setup:
    PreparedStatement statement = connection.prepareStatement(query)
    ResultSet resultSet = runUnderTrace("parent") {
      assert statement.execute()
      return statement.resultSet
    }

    expect:
    resultSet.next()
    resultSet.getInt(1) == 3
    assertTraces(1) {
      trace(0, 2) {
        basicSpan(it, 0, "parent")
        span(1) {
          name JDBCUtils.normalizeSql(query)
          kind CLIENT
          childOf span(0)
          errored false
          attributes {
            "${SemanticAttributes.DB_SYSTEM.key()}" system
            "${SemanticAttributes.DB_NAME.key()}" dbName.toLowerCase()
            if (username != null) {
              "${SemanticAttributes.DB_USER.key()}" username
            }
            "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
            "${SemanticAttributes.DB_CONNECTION_STRING.key()}" url
          }
        }
      }
    }

    cleanup:
    statement.close()
    connection.close()

    where:
    system  | connection                                                | username | query                            | url
    "h2"    | new Driver().connect(jdbcUrls.get("h2"), null)            | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | new EmbeddedDriver().connect(jdbcUrls.get("derby"), null) | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    "h2"    | cpDatasources.get("tomcat").get("h2").getConnection()     | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | cpDatasources.get("tomcat").get("derby").getConnection()  | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    "h2"    | cpDatasources.get("hikari").get("h2").getConnection()     | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | cpDatasources.get("hikari").get("derby").getConnection()  | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    "h2"    | cpDatasources.get("c3p0").get("h2").getConnection()       | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | cpDatasources.get("c3p0").get("derby").getConnection()    | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
  }

  @Unroll
  def "prepared statement query on #driver with #connection.getClass().getCanonicalName() generates a span"() {
    setup:
    PreparedStatement statement = connection.prepareStatement(query)
    ResultSet resultSet = runUnderTrace("parent") {
      return statement.executeQuery()
    }

    expect:
    resultSet.next()
    resultSet.getInt(1) == 3
    assertTraces(1) {
      trace(0, 2) {
        basicSpan(it, 0, "parent")
        span(1) {
          name JDBCUtils.normalizeSql(query)
          kind CLIENT
          childOf span(0)
          errored false
          attributes {
            "${SemanticAttributes.DB_SYSTEM.key()}" system
            "${SemanticAttributes.DB_NAME.key()}" dbName.toLowerCase()
            if (username != null) {
              "${SemanticAttributes.DB_USER.key()}" username
            }
            "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
            "${SemanticAttributes.DB_CONNECTION_STRING.key()}" url

          }
        }
      }
    }

    cleanup:
    statement.close()
    connection.close()

    where:
    system  | connection                                                | username | query                            | url
    "h2"    | new Driver().connect(jdbcUrls.get("h2"), null)            | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | new EmbeddedDriver().connect(jdbcUrls.get("derby"), null) | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    "h2"    | cpDatasources.get("tomcat").get("h2").getConnection()     | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | cpDatasources.get("tomcat").get("derby").getConnection()  | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    "h2"    | cpDatasources.get("hikari").get("h2").getConnection()     | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | cpDatasources.get("hikari").get("derby").getConnection()  | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    "h2"    | cpDatasources.get("c3p0").get("h2").getConnection()       | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | cpDatasources.get("c3p0").get("derby").getConnection()    | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
  }

  @Unroll
  def "prepared call on #driver with #connection.getClass().getCanonicalName() generates a span"() {
    setup:
    CallableStatement statement = connection.prepareCall(query)
    ResultSet resultSet = runUnderTrace("parent") {
      return statement.executeQuery()
    }

    expect:
    resultSet.next()
    resultSet.getInt(1) == 3
    assertTraces(1) {
      trace(0, 2) {
        basicSpan(it, 0, "parent")
        span(1) {
          name JDBCUtils.normalizeSql(query)
          kind CLIENT
          childOf span(0)
          errored false
          attributes {
            "${SemanticAttributes.DB_SYSTEM.key()}" system
            "${SemanticAttributes.DB_NAME.key()}" dbName.toLowerCase()
            if (username != null) {
              "${SemanticAttributes.DB_USER.key()}" username
            }
            "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
            "${SemanticAttributes.DB_CONNECTION_STRING.key()}" url

          }
        }
      }
    }

    cleanup:
    statement.close()
    connection.close()

    where:
    system  | connection                                                | username | query                            | url
    "h2"    | new Driver().connect(jdbcUrls.get("h2"), null)            | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | new EmbeddedDriver().connect(jdbcUrls.get("derby"), null) | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    "h2"    | cpDatasources.get("tomcat").get("h2").getConnection()     | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | cpDatasources.get("tomcat").get("derby").getConnection()  | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    "h2"    | cpDatasources.get("hikari").get("h2").getConnection()     | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | cpDatasources.get("hikari").get("derby").getConnection()  | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    "h2"    | cpDatasources.get("c3p0").get("h2").getConnection()       | null     | "SELECT 3"                       | "h2:mem:"
    "derby" | cpDatasources.get("c3p0").get("derby").getConnection()    | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
  }

  @Unroll
  def "statement update on #driver with #connection.getClass().getCanonicalName() generates a span"() {
    setup:
    Statement statement = connection.createStatement()
    def sql = connection.nativeSQL(query)

    expect:
    runUnderTrace("parent") {
      return !statement.execute(sql)
    }
    statement.updateCount == 0
    assertTraces(1) {
      trace(0, 2) {
        basicSpan(it, 0, "parent")
        span(1) {
          name JDBCUtils.normalizeSql(query)
          kind CLIENT
          childOf span(0)
          errored false
          attributes {
            "${SemanticAttributes.DB_SYSTEM.key()}" system
            "${SemanticAttributes.DB_NAME.key()}" dbName.toLowerCase()
            if (username != null) {
              "${SemanticAttributes.DB_USER.key()}" username
            }
            "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
            "${SemanticAttributes.DB_CONNECTION_STRING.key()}" url

          }
        }
      }
    }

    cleanup:
    statement.close()
    connection.close()

    where:
    system   | connection                                                | username | query                                                                           | url
    "h2"     | new Driver().connect(jdbcUrls.get("h2"), null)            | null     | "CREATE TABLE S_H2 (id INTEGER not NULL, PRIMARY KEY ( id ))"                   | "h2:mem:"
    "derby"  | new EmbeddedDriver().connect(jdbcUrls.get("derby"), null) | "APP"    | "CREATE TABLE S_DERBY (id INTEGER not NULL, PRIMARY KEY ( id ))"                | "derby:memory:"
    "hsqldb" | new JDBCDriver().connect(jdbcUrls.get("hsqldb"), null)    | "SA"     | "CREATE TABLE PUBLIC.S_HSQLDB (id INTEGER not NULL, PRIMARY KEY ( id ))"        | "hsqldb:mem:"
    "h2"     | cpDatasources.get("tomcat").get("h2").getConnection()     | null     | "CREATE TABLE S_H2_TOMCAT (id INTEGER not NULL, PRIMARY KEY ( id ))"            | "h2:mem:"
    "derby"  | cpDatasources.get("tomcat").get("derby").getConnection()  | "APP"    | "CREATE TABLE S_DERBY_TOMCAT (id INTEGER not NULL, PRIMARY KEY ( id ))"         | "derby:memory:"
    "hsqldb" | cpDatasources.get("tomcat").get("hsqldb").getConnection() | "SA"     | "CREATE TABLE PUBLIC.S_HSQLDB_TOMCAT (id INTEGER not NULL, PRIMARY KEY ( id ))" | "hsqldb:mem:"
    "h2"     | cpDatasources.get("hikari").get("h2").getConnection()     | null     | "CREATE TABLE S_H2_HIKARI (id INTEGER not NULL, PRIMARY KEY ( id ))"            | "h2:mem:"
    "derby"  | cpDatasources.get("hikari").get("derby").getConnection()  | "APP"    | "CREATE TABLE S_DERBY_HIKARI (id INTEGER not NULL, PRIMARY KEY ( id ))"         | "derby:memory:"
    "hsqldb" | cpDatasources.get("hikari").get("hsqldb").getConnection() | "SA"     | "CREATE TABLE PUBLIC.S_HSQLDB_HIKARI (id INTEGER not NULL, PRIMARY KEY ( id ))" | "hsqldb:mem:"
    "h2"     | cpDatasources.get("c3p0").get("h2").getConnection()       | null     | "CREATE TABLE S_H2_C3P0 (id INTEGER not NULL, PRIMARY KEY ( id ))"              | "h2:mem:"
    "derby"  | cpDatasources.get("c3p0").get("derby").getConnection()    | "APP"    | "CREATE TABLE S_DERBY_C3P0 (id INTEGER not NULL, PRIMARY KEY ( id ))"           | "derby:memory:"
    "hsqldb" | cpDatasources.get("c3p0").get("hsqldb").getConnection()   | "SA"     | "CREATE TABLE PUBLIC.S_HSQLDB_C3P0 (id INTEGER not NULL, PRIMARY KEY ( id ))"   | "hsqldb:mem:"
  }

  @Unroll
  def "prepared statement update on #driver with #connection.getClass().getCanonicalName() generates a span"() {
    setup:
    def sql = connection.nativeSQL(query)
    PreparedStatement statement = connection.prepareStatement(sql)

    expect:
    runUnderTrace("parent") {
      return statement.executeUpdate() == 0
    }
    assertTraces(1) {
      trace(0, 2) {
        basicSpan(it, 0, "parent")
        span(1) {
          name JDBCUtils.normalizeSql(query)
          kind CLIENT
          childOf span(0)
          errored false
          attributes {
            "${SemanticAttributes.DB_SYSTEM.key()}" system
            "${SemanticAttributes.DB_NAME.key()}" dbName.toLowerCase()
            if (username != null) {
              "${SemanticAttributes.DB_USER.key()}" username
            }
            "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
            "${SemanticAttributes.DB_CONNECTION_STRING.key()}" url

          }
        }
      }
    }

    cleanup:
    statement.close()
    connection.close()

    where:
    system  | connection                                                | username | query                                                                    | url
    "h2"    | new Driver().connect(jdbcUrls.get("h2"), null)            | null     | "CREATE TABLE PS_H2 (id INTEGER not NULL, PRIMARY KEY ( id ))"           | "h2:mem:"
    "derby" | new EmbeddedDriver().connect(jdbcUrls.get("derby"), null) | "APP"    | "CREATE TABLE PS_DERBY (id INTEGER not NULL, PRIMARY KEY ( id ))"        | "derby:memory:"
    "h2"    | cpDatasources.get("tomcat").get("h2").getConnection()     | null     | "CREATE TABLE PS_H2_TOMCAT (id INTEGER not NULL, PRIMARY KEY ( id ))"    | "h2:mem:"
    "derby" | cpDatasources.get("tomcat").get("derby").getConnection()  | "APP"    | "CREATE TABLE PS_DERBY_TOMCAT (id INTEGER not NULL, PRIMARY KEY ( id ))" | "derby:memory:"
    "h2"    | cpDatasources.get("hikari").get("h2").getConnection()     | null     | "CREATE TABLE PS_H2_HIKARI (id INTEGER not NULL, PRIMARY KEY ( id ))"    | "h2:mem:"
    "derby" | cpDatasources.get("hikari").get("derby").getConnection()  | "APP"    | "CREATE TABLE PS_DERBY_HIKARI (id INTEGER not NULL, PRIMARY KEY ( id ))" | "derby:memory:"
    "h2"    | cpDatasources.get("c3p0").get("h2").getConnection()       | null     | "CREATE TABLE PS_H2_C3P0 (id INTEGER not NULL, PRIMARY KEY ( id ))"      | "h2:mem:"
    "derby" | cpDatasources.get("c3p0").get("derby").getConnection()    | "APP"    | "CREATE TABLE PS_DERBY_C3P0 (id INTEGER not NULL, PRIMARY KEY ( id ))"   | "derby:memory:"
  }

  @Unroll
  def "connection constructor throwing then generating correct spans after recovery using #driver connection (prepare statement = #prepareStatement)"() {
    setup:
    Connection connection = null

    when:
    try {
      connection = new TestConnection(true)
    } catch (Exception e) {
      connection = driverClass.connect(jdbcUrl, null)
    }

    Statement statement = null
    ResultSet rs = runUnderTrace("parent") {
      if (prepareStatement) {
        statement = connection.prepareStatement(query)
        return statement.executeQuery()
      }

      statement = connection.createStatement()
      return statement.executeQuery(query)
    }

    then:
    rs.next()
    rs.getInt(1) == 3
    assertTraces(1) {
      trace(0, 2) {
        basicSpan(it, 0, "parent")
        span(1) {
          name JDBCUtils.normalizeSql(query)
          kind CLIENT
          childOf span(0)
          errored false
          attributes {
            if (prepareStatement) {
            } else {
            }
            "${SemanticAttributes.DB_SYSTEM.key()}" system
            "${SemanticAttributes.DB_NAME.key()}" dbName.toLowerCase()
            if (username != null) {
              "${SemanticAttributes.DB_USER.key()}" username
            }
            "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
            "${SemanticAttributes.DB_CONNECTION_STRING.key()}" url

          }
        }
      }
    }

    cleanup:
    if (statement != null) {
      statement.close()
    }
    if (connection != null) {
      connection.close()
    }

    where:
    prepareStatement | system  | driverClass          | jdbcUrl                                        | username | query                            | url
    true             | "h2"    | new Driver()         | "jdbc:h2:mem:" + dbName                        | null     | "SELECT 3;"                      | "h2:mem:"
    true             | "derby" | new EmbeddedDriver() | "jdbc:derby:memory:" + dbName + ";create=true" | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
    false            | "h2"    | new Driver()         | "jdbc:h2:mem:" + dbName                        | null     | "SELECT 3;"                      | "h2:mem:"
    false            | "derby" | new EmbeddedDriver() | "jdbc:derby:memory:" + dbName + ";create=true" | "APP"    | "SELECT 3 FROM SYSIBM.SYSDUMMY1" | "derby:memory:"
  }

  def "calling #datasource.class.simpleName getConnection generates a span when under existing trace"() {
    setup:
    assert datasource instanceof DataSource
    init?.call(datasource)

    when:
    datasource.getConnection().close()

    then:
    !TEST_WRITER.traces.any { it.any { it.name == "database.connection" } }
    TEST_WRITER.clear()

    when:
    runUnderTrace("parent") {
      datasource.getConnection().close()
    }

    then:
    assertTraces(1) {
      trace(0, recursive ? 3 : 2) {
        basicSpan(it, 0, "parent")

        span(1) {
          name "${datasource.class.simpleName}.getConnection"
          childOf span(0)
          attributes {
          }
        }
        if (recursive) {
          span(2) {
            name "${datasource.class.simpleName}.getConnection"
            childOf span(1)
            attributes {
            }
          }
        }
      }
    }

    where:
    datasource                               | init
    new JdbcDataSource()                     | { ds -> ds.setURL(jdbcUrls.get("h2")) }
    new EmbeddedDataSource()                 | { ds -> ds.jdbcurl = jdbcUrls.get("derby") }
    cpDatasources.get("hikari").get("h2")    | null
    cpDatasources.get("hikari").get("derby") | null
    cpDatasources.get("c3p0").get("h2")      | null
    cpDatasources.get("c3p0").get("derby")   | null

    // Tomcat's pool doesn't work because the getConnection method is
    // implemented in a parent class that doesn't implement DataSource

    recursive = datasource instanceof EmbeddedDataSource
  }

  def "test getClientInfo exception"() {
    setup:
    Connection connection = new TestConnection(false)

    when:
    Statement statement = null
    runUnderTrace("parent") {
      statement = connection.createStatement()
      return statement.executeQuery(query)
    }

    then:
    assertTraces(1) {
      trace(0, 2) {
        basicSpan(it, 0, "parent")
        span(1) {
          name JDBCUtils.normalizeSql(query)
          kind CLIENT
          childOf span(0)
          errored false
          attributes {
            "${SemanticAttributes.DB_SYSTEM.key()}" "testdb"
            "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
            "${SemanticAttributes.DB_CONNECTION_STRING.key()}" "testdb://localhost"
          }
        }
      }
    }

    cleanup:
    if (statement != null) {
      statement.close()
    }
    if (connection != null) {
      connection.close()
    }

    where:
    database = "testdb"
    query = "testing 123"
  }

  @Unroll
  def "#connectionPoolName connections should be cached in case of wrapped connections"() {
    setup:
    String dbType = "hsqldb"
    DataSource ds = createDS(connectionPoolName, dbType, jdbcUrls.get(dbType))
    String query = "SELECT 3 FROM INFORMATION_SCHEMA.SYSTEM_USERS"
    int numQueries = 5
    Connection connection = null
    Statement statement = null
    ResultSet rs = null
    int[] res = new int[numQueries]

    when:
    for (int i = 0; i < numQueries; ++i) {
      try {
        connection = ds.getConnection()
        statement = connection.prepareStatement(query)
        rs = statement.executeQuery()
        if (rs.next()) {
          res[i] = rs.getInt(1)
        } else {
          res[i] = 0
        }
      } finally {
        connection.close()
      }
    }

    then:
    for (int i = 0; i < numQueries; ++i) {
      res[i] == 3
    }
    assertTraces(5) {
      trace(0, 1) {
        span(0) {
          name JDBCUtils.normalizeSql(query)
          kind CLIENT
          errored false
          attributes {
            "${SemanticAttributes.DB_SYSTEM.key()}" "hsqldb"
            "${SemanticAttributes.DB_NAME.key()}" dbName.toLowerCase()
            "${SemanticAttributes.DB_USER.key()}" "SA"
            "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
            "${SemanticAttributes.DB_CONNECTION_STRING.key()}" "hsqldb:mem:"

          }
        }
      }
      for (int i = 1; i < numQueries; ++i) {
        trace(i, 1) {
          span(0) {
            name JDBCUtils.normalizeSql(query)
            kind CLIENT
            errored false
            attributes {
              "${SemanticAttributes.DB_SYSTEM.key()}" "hsqldb"
              "${SemanticAttributes.DB_NAME.key()}" dbName.toLowerCase()
              "${SemanticAttributes.DB_USER.key()}" "SA"
              "${SemanticAttributes.DB_STATEMENT.key()}" JDBCUtils.normalizeSql(query)
              "${SemanticAttributes.DB_CONNECTION_STRING.key()}" "hsqldb:mem:"

            }
          }
        }
      }
    }

    cleanup:
    if (ds instanceof Closeable) {
      ds.close()
    }

    where:
    connectionPoolName | _
    "hikari"           | _
    "tomcat"           | _
    "c3p0"             | _
  }
}
