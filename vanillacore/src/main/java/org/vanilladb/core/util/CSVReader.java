package org.vanilladb.core.util;
import java.io.*;
import java.sql.Connection;
import java.util.*;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.query.planner.*;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.sql.Type.DOUBLE;

import java.io.File;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.statistics.StatMgr;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.core.query.parse.*;
import java.util.*;
import org.vanilladb.core.sql.*;

/*
 * This file implements the API to insert data from csv files
 * The schema of table is required in the first of csv as seperated by ','
 * table name is contained in the file as name.csv
 */
public class CSVReader {
    private static final BlockId FLAG_DATA_BLOCK = new BlockId("testing_flags", 0);
    private static final int LOADED_FLAG_POS = 0;
	private static final Constant DATA_LOADED_VALUE = new IntegerConstant(1);

    public void insert(String query){ // "INSERT INTO test2(a,b,c) values(1,2,3)"
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();
        planner.executeUpdate(query, tx);
        tx.commit();
    }

    public String makeInsertSQL(String table, String[] schema, String[] values){
        String sql = "INSERT INTO " + table + "(";
        for(int i=0;i<schema.length;i++){
            sql += schema[i];
            if(i < schema.length-1){
                sql += ",";
            }else{
                sql += ") values(";
            }
        }
        for(int i=0;i<values.length;i++){
            sql += values[i];
            if(i < values.length-1){
                sql += ",";
            }else{
                sql += ")";
            }
        }
        return sql;
    }

    public void readCSV(String csvFile){
        try {
            String strs[] = csvFile.split(".");
            String tableName = strs[0];
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String delimiter = ",";
            String[] values;
            String[] schema;

            line = br.readLine();
            schema = line.split(delimiter);
            while((line = br.readLine()) != null) {
                values = line.split(delimiter);
                String sql = makeInsertSQL(tableName, schema, values);
                System.out.println(sql);
                //insert(sql);
            }
            br.close();
            } catch(IOException ioe) {
               ioe.printStackTrace();
            }
    }

    public Map<String, Type> parseTable(String qry){
        // String qry = "create table Enro11(Eid int, StudentId int, "
		// 		+ "SectionId int, Grade varchar(2)) ";

		Parser parser = new Parser(qry);
		CreateTableData ctd = (CreateTableData) parser.updateCommand();
		Map<String, Type> schema = ctd.newSchema().getSchema();
        System.out.println(ctd.tableName());
        for (Map.Entry<String,Type> entry : schema.entrySet()){
            System.out.println("Key = " + entry.getKey() +
                             ", Value = " + entry.getValue());
        }
        return schema;
    }

    public void loadTable(String createTableSQL, String tableName){
        Map<String, Type> schema = parseTable(createTableSQL);

        CatalogMgr md = VanillaDb.catalogMgr();
        StatMgr stat = VanillaDb.statMgr();
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);

        // create and populate the table
        Schema sch = new Schema();
        List<String> fields = new ArrayList<>();
        List<Type> types = new ArrayList<>();
        for (Map.Entry<String,Type> entry : schema.entrySet()){
            String field = entry.getKey();
            Type value = entry.getValue();
            fields.add(field);
            types.add(value);
            sch.addField(field, value);
        }
        md.createTable(tableName, sch, tx);
        TableInfo ti = md.getTableInfo(tableName, tx);

        RecordFile rf = ti.open(tx, true);
        rf.beforeFirst();
        while (rf.next())
            rf.delete();
        rf.close();

        rf = ti.open(tx, true);

        //populate table 
        //read data from csv
        String path = "/Users/yiminglin/Documents/Codebase/datahub/filterplus/testdata/";
        String csvFile = path + tableName.toLowerCase() + ".csv";
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String delimiter = ",";
            String[] values;
            line = br.readLine();//skip header 
            while((line = br.readLine()) != null) {
                values = line.split(delimiter);
                rf.insert();
                for(int i=0;i<values.length;i++){
                    Type type = types.get(i);
                    String field = fields.get(i);
                    if(type == INTEGER){
                        int value = Integer.valueOf(values[i]);
                        rf.setVal(field, new IntegerConstant(value));
                    }else if(type == DOUBLE){
                        double value = Double.valueOf(values[i]);
                        rf.setVal(field, new DoubleConstant(value));
                    }else if(type == VARCHAR){
                        rf.setVal(field, new VarcharConstant(values[i]));
                    }
                }
            }
            br.close();
            } catch(IOException ioe) {
               ioe.printStackTrace();
            }
        
        rf.close();
        // refresh the statistical information after populating this table
        stat.getTableStatInfo(ti, tx);
        tx.commit();

        tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			RecoveryMgr.initializeSystem(tx);
			tx.commit();
        
        // Set the flag indicating that the data is loaded
        setFlagAsLoaded();
    }

    private void setFlagAsLoaded() {
		Page page = new Page();
		page.setVal(LOADED_FLAG_POS, DATA_LOADED_VALUE);
		page.write(FLAG_DATA_BLOCK);
	}
}
