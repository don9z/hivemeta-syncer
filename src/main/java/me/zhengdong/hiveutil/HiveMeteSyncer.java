package me.zhengdong.hiveutil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HiveMeteSyncer {
    boolean dryRun = false;

    private List<DBInfo> dbs;
    private List<TBLInfo> tbls;
    private List<COLInfo> cols;

    public HiveMeteSyncer() {
        dbs = new ArrayList<DBInfo>();
        tbls = new ArrayList<TBLInfo>();
        cols = new ArrayList<COLInfo>();
    }

    public class DBInfo {
        private long id;
        private String name;

        public DBInfo(long id, String name) {
            this.id = id;
            this.name = name;
        }

        public long getId() {
            return this.id;
        }

        public String getName() {
            return this.name;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + (int) (id ^ (id >>> 32));
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DBInfo other = (DBInfo) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (id != other.id)
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }

        private HiveMeteSyncer getOuterType() {
            return HiveMeteSyncer.this;
        }
    }

    public class TBLInfo {
        private long id;
        private String name;
        private long dbId;

        public TBLInfo(long id, String name, long dbId) {
            this.id = id;
            this.name = name;
            this.dbId = dbId;
        }

        public long getId() {
            return this.id;
        }

        public String getName() {
            return this.name;
        }

        public long getDbId() {
            return this.dbId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + (int) (dbId ^ (dbId >>> 32));
            result = prime * result + (int) (id ^ (id >>> 32));
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TBLInfo other = (TBLInfo) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (dbId != other.dbId)
                return false;
            if (id != other.id)
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }

        private HiveMeteSyncer getOuterType() {
            return HiveMeteSyncer.this;
        }
    }

    public class COLInfo {
        private String colName;
        private String colType;
        private long tblId;

        public COLInfo(String colName, String colType, long tblId) {
            this.colName = colName;
            this.colType = colType;
            this.tblId = tblId;
        }

        public String getColName() {
            return this.colName;
        }

        public String getColType() {
            return this.colType;
        }

        public long getTblId() {
            return this.tblId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result
                    + ((colName == null) ? 0 : colName.hashCode());
            result = prime * result
                    + ((colType == null) ? 0 : colType.hashCode());
            result = prime * result + (int) (tblId ^ (tblId >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            COLInfo other = (COLInfo) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (colName == null) {
                if (other.colName != null)
                    return false;
            } else if (!colName.equals(other.colName))
                return false;
            if (colType == null) {
                if (other.colType != null)
                    return false;
            } else if (!colType.equals(other.colType))
                return false;
            if (tblId != other.tblId)
                return false;
            return true;
        }

        private HiveMeteSyncer getOuterType() {
            return HiveMeteSyncer.this;
        }
    }

    public void addDb(long db_id, String db_name) {
        dbs.add(new DBInfo(db_id, db_name));
    }

    public void addTbl(long tbl_id, String tbl_name, long db_id) {
        tbls.add(new TBLInfo(tbl_id, tbl_name, db_id));
    }

    public void addCol(String col_name, String col_type, long tbl_id) {
        cols.add(new COLInfo(col_name, col_type, tbl_id));
    }

    public List<DBInfo> getDbs() {
        return this.dbs;
    }

    public List<TBLInfo> getTbls() {
        return this.tbls;
    }

    public List<COLInfo> getCols() {
        return this.cols;
    }

    public void fetch(String metastore_url, String user, String passwd)
            throws SQLException {
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            JDBCHelper.loadMySQLJDBCDriver();
            con = DriverManager.getConnection(JDBCHelper.mysqlJDBCProtocol
                    + metastore_url, user, passwd);
            stmt = con.createStatement();

            String dbQuerySQL = "SELECT DB_ID, NAME FROM DBS";
            String tblQuerySQL = "SELECT TBL_ID, TBL_NAME, DB_ID, SD_ID FROM TBLS";
            String colQuerySQL = "SELECT COLUMN_NAME, TYPE_NAME FROM COLUMNS_V2";

            res = stmt.executeQuery(dbQuerySQL);
            while (res.next()) {
                this.addDb(res.getLong(1), res.getString(2));
            }

            ArrayList<Long> sds = new ArrayList<Long>();
            res = stmt.executeQuery(tblQuerySQL);
            while (res.next()) {
                this.addTbl(res.getLong(1), res.getString(2), res.getLong(3));
                sds.add(res.getLong(4));
            }

            for (int i = 0; i < sds.size(); i++) {
                res = stmt.executeQuery(colQuerySQL +
                        " WHERE CD_ID = (select CD_ID from SDS where SD_ID = "
                        + sds.get(i).toString() + ")");
                while (res.next()) {
                    this.addCol(res.getString(1), res.getString(2), this.tbls
                            .get(i).getId());
                }
            }
        } finally {
            JDBCHelper.close(res, stmt, con);
        }
    }

    public static <T> Collection<T> substract(Collection<T> left,
            Collection<T> right) {
        Collection<T> result = new ArrayList<T>(left);
        result.removeAll(right);
        return result;
    }

    public int executeUpdateSql(Statement stmt, String sql) throws SQLException {
        System.out.println(sql);
        if (!this.dryRun) {
            return stmt.executeUpdate(sql);
        }
        return 0;
    }

    public void exportToDb(String db_url, String user, String passwd)
            throws SQLException {
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            JDBCHelper.loadMySQLJDBCDriver();
            con = DriverManager.getConnection(JDBCHelper.mysqlJDBCProtocol
                    + db_url, user, passwd);
            stmt = con.createStatement();

            String dbQuerySql = "SELECT id, db_name FROM hivemeta_database";
            String tblQuerySql = "SELECT id, tbl_name, db_id FROM hivemeta_table";
            String colQuerySql = "SELECT col_name, col_type, table_id FROM hivemeta_column";

            List<DBInfo> previousDbExported = new ArrayList<DBInfo>();
            res = stmt.executeQuery(dbQuerySql);
            while (res.next()) {
                DBInfo db = new DBInfo(res.getLong(1), res.getString(2));
                previousDbExported.add(db);
            }

            List<TBLInfo> previousTblExported = new ArrayList<TBLInfo>();
            res = stmt.executeQuery(tblQuerySql);
            while (res.next()) {
                TBLInfo tbl = new TBLInfo(res.getLong(1), res.getString(2),
                        res.getLong(3));
                previousTblExported.add(tbl);
            }

            List<COLInfo> previousColExported = new ArrayList<COLInfo>();
            res = stmt.executeQuery(colQuerySql);
            while (res.next()) {
                COLInfo col = new COLInfo(res.getString(1), res.getString(2),
                        res.getLong(3));
                previousColExported.add(col);
            }

            Collection<DBInfo> dbsShouldAdd = HiveMeteSyncer.substract(
                    this.dbs, previousDbExported);
            Collection<DBInfo> dbsShouldDel = HiveMeteSyncer.substract(
                    previousDbExported, this.dbs);
            Collection<TBLInfo> tblsShouldAdd = HiveMeteSyncer.substract(
                    this.tbls, previousTblExported);
            Collection<TBLInfo> tblsShouldDel = HiveMeteSyncer.substract(
                    previousTblExported, this.tbls);
            Collection<COLInfo> colsShouldAdd = HiveMeteSyncer.substract(
                    this.cols, previousColExported);
            Collection<COLInfo> colsShouldDel = HiveMeteSyncer.substract(
                    previousColExported, this.cols);

            String dbInsertSql = "INSERT INTO hivemeta_database VALUES";
            String tblInsertSql = "INSERT INTO hivemeta_table VALUES";
            String colInsertSql = "INSERT INTO hivemeta_column (col_name, col_type, table_id) VALUES";
            String dbDeleteSql = "DELETE FROM hivemeta_database WHERE";
            String tblDeleteSql = "DELETE FROM hivemeta_table WHERE";
            String colDeleteSql = "DELETE FROM hivemeta_column WHERE";

            System.out.println("Executing:");
            // Insert
            for (DBInfo db : dbsShouldAdd) {
                this.executeUpdateSql(stmt, String.format("%s (%d, '%s')", dbInsertSql,
                        db.getId(), db.getName()));
            }
            for (TBLInfo tbl : tblsShouldAdd) {
                this.executeUpdateSql(stmt, String.format("%s (%d, '%s', %d)",
                        tblInsertSql, tbl.getId(), tbl.getName(), tbl.getDbId()));
            }
            for (COLInfo col : colsShouldAdd) {
                this.executeUpdateSql(stmt, String.format("%s ('%s', '%s', %d)",
                        colInsertSql, col.getColName(), col.getColType(),
                        col.getTblId()));
            }
            // Delete
            for (COLInfo col : colsShouldDel) {
                this.executeUpdateSql(stmt, String.format(
                        "%s col_name='%s' AND table_id=%d", colDeleteSql,
                        col.getColName(), col.getTblId()));
            }
            for (TBLInfo tbl : tblsShouldDel) {
                this.executeUpdateSql(stmt, String.format("%s id=%d", tblDeleteSql,
                        tbl.getId()));
            }
            for (DBInfo db : dbsShouldDel) {
                this.executeUpdateSql(stmt, String.format("%s id=%d", dbDeleteSql,
                        db.getId()));
            }
        } finally {
            JDBCHelper.close(res, stmt, con);
        }
    }

    private static void printUsage() {
        System.out
                .println("usage: HiveMeteSyncer [--dry-run] from_hive_url "
                        + "hive_metastore_user hive_metastore_user_passwd to_mysql_url "
                        + "mysql_user mysql_user_passwd");
        System.out.println(
                "example: HiveMeteSyncer 172.19.0.109:4306/metastore " +
                        "hiveuser hivepassword 172.19.0.109:4306/mysql username password \n" +
                "         java -jar target/hivemeta-syncer-1.0-jar-with-dependencies.jar 172.19.0.109:4306/metastore " +
                        "hiveuser hivepassword 172.19.0.109:4306/mysql username password");
    }

    public static void main(String args[]) {
        HiveMeteSyncer fetcher = new HiveMeteSyncer();

        if (args.length == 7 && args[0].compareToIgnoreCase("--dry-run") == 0) {
            fetcher.dryRun = true;
            args = new String[]{args[1], args[2], args[3], args[4], args[5], args[6]};
        }

        if (args.length == 6) {
            try {
                fetcher.fetch(args[0], args[1], args[2]);
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }

            try {
                fetcher.exportToDb(args[3], args[4], args[5]);
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        } else {
            printUsage();
        }
    }
}
