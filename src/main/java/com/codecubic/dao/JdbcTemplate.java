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

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JdbcTemplate {
    private static String SELECT_TEMPLAT = "select * from %s limit 1";
    private JdbcConfig _jdbcConfig;
    private Connection _conn;

    public JdbcTemplate(JdbcConfig jdbcConfig) {
        this._jdbcConfig = jdbcConfig;
        try {
            Class.forName(_jdbcConfig.getDriver());
        } catch (Exception e) {
            log.error("", e);
            e.printStackTrace();
            System.exit(-1);
        }
    }


    private synchronized Connection getConn() throws SQLException {
        if (_conn == null) {
            try {
                _conn = DriverManager.getConnection(this._jdbcConfig.getUrl(), this._jdbcConfig.getUser(), this._jdbcConfig.getPasswd());
            } catch (SQLException e) {
                throw new SQLException("Connect to MySql Server Error : " + e.getMessage());
            }
        }
        return _conn;
    }


    /**
     * wh:执行非查询类SQL
     *
     * @param sql
     */
    public void execute(String sql) throws SQLException {
        Connection conn = getConn();
        Statement stat = conn.createStatement();
        stat.execute(sql);
    }

    /**
     * 获取表中所有字段名称
     *
     * @param tableName 表名
     * @return
     */
    private List<String> getAllColNames(String tableName) {
        List<String> columnNames = new ArrayList<>();
        try (PreparedStatement pStemt = _conn.prepareStatement(String.format(SELECT_TEMPLAT, tableName))) {
            // 结果集元数据
            ResultSetMetaData rsmd = pStemt.getMetaData();
            // 表列数
            int size = rsmd.getColumnCount();
            for (int i = 0; i < size; i++) {
                columnNames.add(rsmd.getColumnName(i + 1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnNames;
    }

    public List<ColumnMeta> getAllCols(String schema, String tabName) {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        try {
            //获取数据库的元数据
            DatabaseMetaData dbMetaData = _conn.getMetaData();
            //从元数据中获取到所有的表名
            ResultSet colRet = dbMetaData.getColumns(null, schema, tabName, "%");
            while (colRet.next()) {
                ColumnMeta columnMeta = new ColumnMeta();
                columnMeta.setName(colRet.getString("COLUMN_NAME"));
                columnMeta.setType(colRet.getString("TYPE_NAME"));
                columnMeta.setSize(colRet.getInt("COLUMN_SIZE"));
                columnMeta.setDigits(colRet.getInt("DECIMAL_DIGITS"));
                int nullable = colRet.getInt("NULLABLE");
                columnMeta.setNullAble(nullable == 1 ? true : false);
                columnMetas.add(columnMeta);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return columnMetas;
    }

    public TableMeta getTabMeta(String schema, String tabName) throws SQLException {
        //获取数据库的元数据
        DatabaseMetaData dbMetaData = _conn.getMetaData();
        //从元数据中获取到所有的表名
        ResultSet rs = dbMetaData.getTables(null, schema, tabName, new String[]{"TABLE"});
        while (rs.next()) {
            TableMeta meta = new TableMeta();
            meta.setName(rs.getString("TABLE_NAME"));
            meta.setType(rs.getString("TABLE_TYPE"));
            meta.setCat(rs.getString("TABLE_CAT"));
            meta.setUserName(rs.getString("TABLE_SCHEM"));
            meta.setRemark(rs.getString("REMARKS"));
            return meta;
        }
        return new TableMeta();
    }


    public synchronized void close() {
        if (_conn != null) {
            try {
                _conn.close();
            } catch (SQLException e) {
            } finally {
                _conn = null;
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
                    _conn,
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
                    this._conn,
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
