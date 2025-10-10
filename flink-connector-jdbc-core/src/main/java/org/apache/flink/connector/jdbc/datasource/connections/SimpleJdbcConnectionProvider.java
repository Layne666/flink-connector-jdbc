/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.datasource.connections;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Simple JDBC connection provider. */
@NotThreadSafe
@PublicEvolving
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);
    private static final long serialVersionUID = 1L;

    private final JdbcConnectionOptions jdbcOptions;
    private final String connectionKey; // Unique key for this provider's specific DB connection.

    // Use a static volatile field for the singleton DataSource
    private static final Map<String, HikariDataSource> dataSourceMap = new ConcurrentHashMap<>();

    private transient Connection connection;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();

        // Add a single shutdown hook to cleanly close ALL connection pools when the JVM exits.
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    synchronized (dataSourceMap) {
                                        if (!dataSourceMap.isEmpty()) {
                                            LOG.info(
                                                    "Closing {} shared HikariCP DataSource(s) due to JVM shutdown...",
                                                    dataSourceMap.size());
                                            dataSourceMap.values().forEach(HikariDataSource::close);
                                            dataSourceMap.clear();
                                        }
                                    }
                                }));
    }

    public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
        LOG.info("Using custom HikariCP-based SimpleJdbcConnectionProvider.");
        // Create a unique key based on URL and username to identify a specific database connection.
        this.connectionKey = jdbcOptions.getDbURL() + "::" + jdbcOptions.getUsername().orElse("");
    }

    private HikariDataSource getDataSource() {
        // Use computeIfAbsent for thread-safe, efficient, and atomic initialization of the
        // connection pool for a given key.
        return dataSourceMap.computeIfAbsent(
                this.connectionKey,
                key -> {
                    LOG.info("No existing HikariCP pool for key '{}'. Creating a new one.", key);
                    return createHikariDataSource(
                            jdbcOptions.getDbURL(),
                            jdbcOptions.getUsername().orElse(null),
                            jdbcOptions.getPassword().orElse(null));
                });
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        if (connection == null) {
            LOG.info("Connection is null, so it is invalid.");
            return false;
        }
        try {
            if (connection.isClosed()) {
                LOG.info("Connection is closed, so it is invalid.");
                return false;
            }
            // First quickly check whether the connection is alive
            boolean valid = connection.isValid(3); // Check validity with a 3-seconds timeout
            if (!valid) {
                LOG.info("Connection is invalid.");
                return false;
            }
            // If the connection is alive, check whether it is usable
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1");
            }
            return true;
        } catch (Exception e) {
            LOG.error("Failed to validate connection, msg:{}", e.getMessage());
            return false;
        }
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException {
        HikariDataSource ds = getDataSource();
        int maxRetries = 5;
        long baseSleepMs = 500;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            Connection conn = null;
            try {
                conn = ds.getConnection();
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SELECT 1");
                }
                // Successfully obtained and verified, return connection
                this.connection = conn;
                return conn;
            } catch (Exception ex) {
                LOG.warn("Attempt {} to get valid connection failed: {}", attempt, ex.getMessage());
                if (attempt == maxRetries) {
                    LOG.error(
                            "Failed to get valid connection from HikariCP pool for key '{}' after {} attempts",
                            this.connectionKey,
                            maxRetries,
                            ex);
                    throw new RuntimeException(
                            "Failed to get valid connection from HikariCP pool for key '"
                                    + this.connectionKey
                                    + "'",
                            ex);
                }
                // Recycle abnormal connection
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (Exception ignored) {
                }
                // Exponential backoff
                try {
                    Thread.sleep(baseSleepMs * attempt);
                } catch (InterruptedException ignored) {
                }
            }
        }
        throw new RuntimeException("Unreachable code");
    }

    @Override
    public void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                // Returns the connection to its specific shared pool.
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception while closing connection (returning to pool)", e);
        } finally {
            connection = null;
        }
    }

    @Override
    public Connection reestablishConnection() throws SQLException {
        // The pool handles re-establishment. Just get a (potentially new) connection.
        closeConnection();
        return getOrEstablishConnection();
    }

    public HikariDataSource createHikariDataSource(String url, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");

        config.setMaximumPoolSize(512); // Maximum number of connections
        config.setMinimumIdle(16); // Minimal idle connection
        config.setIdleTimeout(
                180_000); // Idle connection timeout, in milliseconds (3 minutes) - much less than
        // wait_timeout, avoid using zombie connections
        config.setMaxLifetime(
                300_000); // Maximum connection survival time in milliseconds (5 minutes) - less
        // than MySQL wait_timeout
        config.setConnectionTimeout(30_000); // Timeout to get connection (30 seconds)

        config.setConnectionTestQuery("SELECT 1"); // Queries for checksum and keep-alive are used
        config.setValidationTimeout(5_000); // Timeout for connection detection (5 seconds)
        config.setKeepaliveTime(
                15_000); // Wake up idle connections every 15 seconds to prevent database
        // disconnection

        config.setPoolName("Flink-Hikari-Connection-Pool");
        return new HikariDataSource(config);
    }
}
