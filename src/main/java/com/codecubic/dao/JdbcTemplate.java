package com.codecubic.dao;

import com.codecubic.model.ColumnMeta;
import com.codecubic.model.JdbcConfig;
import com.codecubic.model.TableMeta;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.BeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class JdbcTemplate {
    private JdbcConfig jdbcConfig;
    private Connection conn;
    private HiveMetaStoreClient hiveMetaClient;
    private HiveConf hiveConf;


    public JdbcTemplate(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
        try {
            Class.forName(this.jdbcConfig.getDriver());
            hiveConf = new HiveConf();
            hiveConf.addResource("hive-site.xml");
            hiveMetaClient = new HiveMetaStoreClient(hiveConf);
        } catch (Exception e) {
            log.error("", e);
        }
    }


    private synchronized Connection getConn() throws SQLException {
        if (this.conn == null) {
            try {
                this.conn = DriverManager.getConnection(this.jdbcConfig.getUrl(), this.jdbcConfig.getUser(), this.jdbcConfig.getPasswd());
            } catch (SQLException e) {
                throw new SQLException("Connect to MySql Server Error : " + e.getMessage());
            }
        }
        return this.conn;
    }

    /**
     * 获取数据库
     */
    public List<String> getAllDatabases() {
        List<String> allDatabases = new ArrayList<>(0);
        try {
            allDatabases = this.hiveMetaClient.getAllDatabases();
        } catch (TException e) {
            e.printStackTrace();
        }
        return allDatabases;
    }

    /**
     * 获取指定数据库所有表
     *
     * @param dbName
     * @return
     */
    public List<String> getTables(String dbName) {
        List<String> tables = new ArrayList<>(0);
        try {
            tables = this.hiveMetaClient.getAllTables(dbName);
        } catch (MetaException e) {
            e.printStackTrace();
        }
        return tables;
    }

    /**
     * 执行非查询类SQL
     *
     * @param sql
     */
    public void execute(String sql) throws SQLException {
        Connection conn = getConn();
        Statement stat = conn.createStatement();
        stat.execute(sql);
    }


    public List<ColumnMeta> getColMetas(String database, String tabName) throws SQLException, TException {
        Table table = hiveMetaClient.getTable(database, tabName);
        List<FieldSchema> cols = table.getSd().getCols();
        List<ColumnMeta> columnMetas = cols.stream().map(e -> {
            ColumnMeta columnMeta = new ColumnMeta();
            columnMeta.setName(e.getName());
            columnMeta.setType(e.getType());
            columnMeta.setRemark(e.getComment());
            return columnMeta;
        }).collect(Collectors.toList());
        if (table.isSetPartitionKeys()) {
            columnMetas.addAll(table.getPartitionKeys().stream().map(e -> {
                ColumnMeta columnMeta = new ColumnMeta();
                columnMeta.setName(e.getName());
                columnMeta.setType(e.getType());
                columnMeta.setRemark(e.getComment());
                columnMeta.setPartitionCol(true);
                return columnMeta;
            }).collect(Collectors.toList()));
        }
        return columnMetas;
    }

    public TableMeta getTabMeta(String schema, String tabName) throws SQLException {
        Connection conn = getConn();
        //获取数据库的元数据
        DatabaseMetaData dbMetaData = conn.getMetaData();
        //从元数据中获取到所有的表名
        ResultSet rs = dbMetaData.getTables(null, schema, tabName, new String[]{"TABLE"});
        while (rs.next()) {
            TableMeta meta = new TableMeta();
            meta.setName(rs.getString("TABLE_NAME"));
            meta.setType(rs.getString("TABLE_TYPE"));
            meta.setDatabase(rs.getString("TABLE_CAT"));
            meta.setUserName(rs.getString("TABLE_SCHEM"));
            meta.setRemark(rs.getString("REMARKS"));
            List<ColumnMeta> allCols = null;
            try {
                allCols = getColMetas(schema, tabName);
            } catch (TException e) {
                log.error("", e);
            }
            meta.setColMetas(allCols);
            return meta;
        }
        return new TableMeta();
    }


    public synchronized void close() {
        if (this.conn != null) {
            try {
                this.conn.close();
                this.hiveMetaClient.close();
            } catch (SQLException e) {
            } finally {
                this.conn = null;
            }
        }
    }

    public List<Map<String, Object>> list(String sql) {
        try {
            Connection conn = getConn();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();
            ArrayList<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                HashMap map = new HashMap();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    map.putIfAbsent(metaData.getColumnName(i), rs.getObject(i));
                }
                list.add(map);
            }
            rs.close();
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return new ArrayList<>(0);
    }

    public <T> List<T> list(String sql, Class<T> clazz, Map map) {
        try {
            QueryRunner qRunner = new QueryRunner();
            List<T> query = (List<T>) qRunner.query(
                    this.conn,
                    sql,
                    new BeanListHandler(
                            clazz, new BasicRowProcessor(new BeanProcessor(map))));
            if (!query.isEmpty()) {
                return query;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return new ArrayList<>(0);
    }

    public <T> T getOne(String sql, Class<T> clazz, Map map) {
        try {

            QueryRunner qRunner = new QueryRunner();
            T query = (T) qRunner.query(
                    this.conn,
                    sql,
                    new BeanHandler(
                            clazz, new BasicRowProcessor(new BeanProcessor(map))));
            if (query != null) {
                return query;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return null;
    }

    public long count(String sql) {
        try {
            Connection conn = getConn();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return -1;
    }


    public void createDBIfNotExist(String schema) throws SQLException {
        ResultSet rs = getConn().getMetaData().getCatalogs();
        boolean exist = false;
        while (rs.next()) {
            if (schema.equalsIgnoreCase(rs.getString("TABLE_CAT"))) {
                exist = true;
                break;
            }
        }
        if (!exist) {
            String createSql = "create database " + schema;
            log.info(createSql);
            execute(createSql);
        }
    }
}
