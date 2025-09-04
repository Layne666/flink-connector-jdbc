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

/** Simple JDBC connection provider. */
@NotThreadSafe
@PublicEvolving
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);
    private static final long serialVersionUID = 1L;

    private final JdbcConnectionOptions jdbcOptions;

    private transient Driver loadedDriver;
    private transient HikariDataSource dataSource;
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
    }

    public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
        LOG.info("Using custom HikariCP-based SimpleJdbcConnectionProvider.");
    }

    @Override
    public Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (Exception e) {
            LOG.error("Failed to get connection from HikariCP", e);
            return null;
        }
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
            try (Statement statement = connection.createStatement()) {
                statement.execute(dataSource.getConnectionTestQuery());
            }
            return true;
        } catch (Exception e) {
            LOG.error("Failed to validate connection, msg:{}", e.getMessage());
            return false;
        }
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException {
        if (dataSource == null) {
            LOG.info("Initializing HikariCP DataSource...");
            dataSource =
                    this.createHikariDataSource(
                            jdbcOptions.getDbURL(),
                            jdbcOptions.getUsername().orElse(null),
                            jdbcOptions.getPassword().orElse(null));
        }
        connection = dataSource.getConnection();
        return connection;
    }

    @Override
    public void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("Failed to close connection", e);
        }
        connection = null;
    }

    @Override
    public Connection reestablishConnection() throws SQLException {
        closeConnection();
        return getOrEstablishConnection();
    }

    public HikariDataSource createHikariDataSource(String url, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");

        // 连接池大小
        config.setMaximumPoolSize(5); // 最大连接数
        config.setMinimumIdle(1); // 最小空闲连接
        config.setIdleTimeout(180_000); // 空闲连接超时时间，单位毫秒（3分钟）- 远小于 wait_timeout，避免用到僵尸连接
        config.setMaxLifetime(300_000); // 连接最大存活时间，单位毫秒（5分钟） - 小于 MySQL wait_timeout（500s）
        config.setConnectionTimeout(10_000); // 获取连接的超时时间，单位毫秒（10秒）

        config.setConnectionTestQuery("SELECT 1"); // 用于校验连接是否可用
        config.setValidationTimeout(3_000); // 连接检测的超时时间（3秒）
        config.setKeepaliveTime(60_000); // 每1分钟唤醒一次空闲连接，防止数据库断开

        config.setPoolName("Flink-Hikari-Connection-Pool");
        return new HikariDataSource(config);
    }
}
