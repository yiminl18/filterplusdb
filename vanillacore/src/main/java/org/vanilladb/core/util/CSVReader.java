package org.vanilladb.core.util;
import java.io.*;
import java.sql.Connection;
import java.util.*;  
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.query.planner.*;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.DOUBLE;
import static org.vanilladb.core.sql.Type.BIGINT;
import org.vanilladb.core.query.planner.index.*;
import java.io.File;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.statistics.StatMgr;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.core.query.parse.*;
import org.vanilladb.core.sql.*;

/*
 * This file implements the API to insert data from csv files
 * The schema of table is required in the first of csv as seperated by ','
 * table name is contained in the file as name.csv
 */
public class CSVReader {

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


    public Map<String, Type> parseTable(String qry){
        // String qry = "create table Enro11(Eid int, StudentId int, "
		// 		+ "SectionId int, Grade varchar(2)) ";

		Parser parser = new Parser(qry);
		CreateTableData ctd = (CreateTableData) parser.updateCommand();
		Map<String, Type> schema = ctd.newSchema().getSchema();
        // System.out.println(ctd.tableName());
        // for (Map.Entry<String,Type> entry : schema.entrySet()){
        //     System.out.println("Key = " + entry.getKey() +
        //                      ", Value = " + entry.getValue());
        // }
        return schema;
    }

    public String clean(String str){
        if(!str.equals("")){
            return str.strip().toLowerCase();
        }
        return str.toLowerCase();
    }

    public void loadTable(String createTableSQL, String tbName, String csvFilePath, List<String> fieldNames){
        int limit = 10000;
        Map<String, Type> schema = parseTable(createTableSQL);

        CatalogMgr md = VanillaDb.catalogMgr();
        StatMgr stat = VanillaDb.statMgr();
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);

        // create and populate the table
        Schema sch = new Schema();
        for (Map.Entry<String,Type> entry : schema.entrySet()){
            String field = entry.getKey();
            Type value = entry.getValue();
            //System.out.println(value);
            sch.addField(field, value);
        }
        md.createTable(tbName, sch, tx);
        

        String idxName = "";
        List<String> indexedFlds = new LinkedList<String>();
        List<String> fields = new ArrayList<>();
        List<Constant> vals = new ArrayList<>();

        //create index 
        for(String fieldName : fieldNames){
            indexedFlds = new LinkedList<String>();
            indexedFlds.add(fieldName);
            idxName = "idx_" + fieldName;
            md.createIndex(idxName, tbName, indexedFlds, IndexType.BTREE, tx);
        }
        
            
        

        TableInfo ti = md.getTableInfo(tbName, tx);

        //populate table 
        //read data from csv
        String csvFile = csvFilePath + tbName.toLowerCase() + ".csv";
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String delimiter = "\\|";
            String[] values;
            line = br.readLine();
            String[] header = line.split(delimiter);
            //System.out.println(line);
            //System.out.println(header);
            int idx = 0;
            while((line = br.readLine()) != null) {
                //System.out.println(line);
                idx += 1;
                if(idx > limit){
                    break;
                }
                if(idx%1000 == 0){
                    System.out.println(idx);
                }
                values = line.split(delimiter);//values are a set of value in one tuple
                //scan each value in a tuple
                fields = new ArrayList<>();
                vals = new ArrayList<>();
                for(int i=0;i<values.length;i++){
                    String field = header[i];
                    if(!schema.containsKey(field)){
                        System.out.println("SCHEMA NOT MATCHED!");
                    }
                    Type type = schema.get(field);
                    //System.out.println(type);
                    String rawValue = clean(values[i]);
                    fields.add(field);
                    //System.out.println(field + " " + rawValue);
                    Constant val= null;
                    if(type == INTEGER){
                        if(rawValue == ""){//missing value 
                            //rf.setVal(field, new IntegerConstant(0));
                            val = new IntegerConstant(0);
                        }else{
                            int value = Integer.valueOf(rawValue);
                            val = new IntegerConstant(value);
                        }
                    }else if(type == DOUBLE){
                        if(rawValue == ""){
                            val =  new DoubleConstant(0);
                        }else{
                            double value = Double.valueOf(rawValue);
                            val = new DoubleConstant(value);
                        }
                    }else if(type == BIGINT){
                        if(rawValue == ""){
                            val =  new BigIntConstant(Long.valueOf(0));
                        }else{
                            val = new BigIntConstant(Long.valueOf(rawValue));
                        }
                    }else{
                        //varchar 
                        val = new VarcharConstant(rawValue);
                    }
                    vals.add(val);
                }
                InsertData data = new InsertData(tbName, fields,vals);
				new IndexUpdatePlanner().executeInsert(data, tx);
            }
            br.close();
            } catch(IOException ioe) {
               ioe.printStackTrace();
            }
        stat.getTableStatInfo(ti, tx);
        tx.commit();
        //add a checkpoint record to limit rollback
        tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);
        RecoveryMgr.initializeSystem(tx);
        tx.commit();
    }

}
