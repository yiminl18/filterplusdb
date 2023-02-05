package org.vanilladb.core;

import java.sql.Connection;
import java.util.*;
import java.io.File;
import java.io.*;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.filter.filterPlan;
import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.query.planner.*;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.util.CSVReader;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.filter.filterPlan;
import org.vanilladb.core.query.planner.JoinKnob;

public class testrun {

    // Flags
	private static final BlockId FLAG_DATA_BLOCK = new BlockId("testing_flags", 0);
	private static final int LOADED_FLAG_POS = 0;
	private static final Constant DATA_LOADED_VALUE = new IntegerConstant(1);

    public static void init(String dbname){
        ServerInit.init(dbname);
    }

    public static void loadData(){
        ServerInit.loadTestbed();
    }

    public static void resetDb(String dbname){
        ServerInit.resetDb(dbname);
    }

    public static void setFlagAsLoaded() {
		Page page = new Page();
		page.setVal(LOADED_FLAG_POS, DATA_LOADED_VALUE);
		page.write(FLAG_DATA_BLOCK);
	}


    public static void testTableAPI(){
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		Plan p = new TablePlan("student", tx);
		Scan s = p.open();
		int id = 0;
		s.beforeFirst();
		while (s.next()) {
			System.out.println(s.getVal("sid") + " " + s.getVal("sname").asJavaVal() + " " + s.getVal("gradyear"));
			id++;
		}
        System.out.println(id);
		s.close();
        tx.commit();
    }

    public static void testTableSQL(){
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
        Planner planner = VanillaDb.newPlanner();
        String query = "select p_partkey, p_size from part where p_size>49";
        Plan plan = planner.createQueryPlan(query, tx);
        Scan s = plan.open();
        s.beforeFirst();
        while(s.next()){
            System.out.println(s.getVal("p_partkey") + " " + s.getVal("p_size").asJavaVal() );
        }
        s.close();
        tx.commit();
    }

    public static void testJoinSQL(){
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
        Planner planner = VanillaDb.newPlanner();
        String query = "select sname, gradyear, did from student, dept where majorid > did and sid>500 and gradyear < 2000";
        Plan plan = planner.createQueryPlan(query, tx);
        Scan s = plan.open();
        s.beforeFirst();
        while(s.next()){
            System.out.println(s.getVal("sname") + " " + s.getVal("gradyear").asJavaVal() + " " + s.getVal("did"));
        }
        s.close();
        tx.commit();
    }

    public static void createIndex(String tableName, String fieldName){
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();
        String idxName = "idx_" + fieldName;
        String query = "CREATE INDEX " + idxName + " ON "  + tableName + " (" + fieldName + ")";
        planner.executeUpdate(query, tx);
        tx.commit();
        
        // tx = VanillaDb.txMgr().newTransaction(
		// 			Connection.TRANSACTION_SERIALIZABLE, false);
			//RecoveryMgr.initializeSystem(tx);
			//tx.commit();
        
    }

    public static void createIndexByCode(String tableName, String fieldName){
        CatalogMgr md = VanillaDb.catalogMgr();
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
        List<String> indexedFlds = new LinkedList<String>();
        indexedFlds.add(fieldName);
        String idxName = "idx_" + fieldName;
        md.createIndex(idxName, tableName, indexedFlds, IndexType.BTREE, tx);
        tx.commit();
    }

    public static void deleteIndex(String idxName, String tableName, String fieldName){
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();
        String query = "DROP INDEX " + idxName + " ON "  + tableName + " (" + fieldName + ")";
        planner.executeUpdate(query, tx);
        tx.commit();
        tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			RecoveryMgr.initializeSystem(tx);
			tx.commit();
        // Set the flag indicating that the data is loaded
		setFlagAsLoaded();
    }

    public static void test(){
        HashMap<Constant, Boolean> m = new HashMap<>();
        m.put(new IntegerConstant(1),true);
        m.put(new IntegerConstant(2), false);
        if(m.containsKey(new IntegerConstant(1))){
            System.out.print("yes");
        }
    }

    public static void testInsert(){
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();
        String query = "INSERT INTO test2(a,b,c) values(1,2,3)";
        planner.executeUpdate(query, tx);
        tx.commit();
    }

    public static void testCreateTable(){
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
        Planner planner = VanillaDb.newPlanner();
        String query = "create table test2(a int, b int, c int)";
        planner.executeUpdate(query, tx);
        tx.commit();
    }

    public static void testReadCSV(){
        // String sql = "CREATE TABLE PART (" + 
        //     "P_PARTKEY		int," + 
        //     "P_NAME			varchar(55)," + 
        //     "P_MFGR			varchar(25)," + 
        //     "P_BRAND			varchar(10)," + 
        //     "P_TYPE			varchar(25)," + 
        //     "P_SIZE			int," + 
        //     "P_CONTAINER		varchar(10)," + 
        //     "P_RETAILPRICE	double," + 
        //     "P_COMMENT		varchar(23))" ;
        // String sql = "CREATE TABLE SUPPLIER (" + 
        // "S_SUPPKEY		int," + 
        // "S_NAME			varchar(25)," + 
        // "S_ADDRESS		varchar(40)," + 
        // "S_NATIONKEY		long, " + 
        // "S_PHONE			varchar(15)," + 
        // "S_ACCTBAL		double," + 
        // "S_COMMENT		varchar(101))"; 
        // String sql = "CREATE TABLE PARTSUPP (" + 
        // "PS_PARTKEY		long  , " + 
        // "PS_SUPPKEY		long  , " + 
        // "PS_AVAILQTY		int," + 
        // "PS_SUPPLYCOST	 double," + 
        // "PS_COMMENT		varchar(199))";
        // String sql = "CREATE TABLE CUSTOMER (" + 
        // "C_CUSTKEY		int," + 
        // "C_NAME			varchar(25)," + 
        // "C_ADDRESS		varchar(40)," + 
        // "C_NATIONKEY		long, " + 
        // "C_PHONE			varchar(15)," + 
        // "C_ACCTBAL		 double," + 
        // "C_MKTSEGMENT	varchar(10)," + 
        // "C_COMMENT		varchar(117))";
        // String sql = "CREATE TABLE ORDERS (" +
        // "O_ORDERKEY		int," +
        // "O_CUSTKEY		long," +
        // "O_ORDERSTATUS	varchar(1)," +
        // "O_TOTALPRICE	 double," +
        // "O_ORDERDATE		 varchar(50)," +
        // "O_ORDERPRIORITY	varchar(15)," +
        // "O_CLERK			varchar(15)," +
        // "O_SHIPPRIORITY	int," +
        // "O_COMMENT		varchar(79))";
        String sql = "CREATE TABLE LINEITEM (" +
        "L_ORDERKEY		long," +
        "L_PARTKEY		long, " +
        "L_SUPPKEY		long, " +
        "L_LINENUMBER	int," +
        "L_QUANTITY		 double," +
        "L_EXTENDEDPRICE	 double," +
        "L_DISCOUNT		 double," +
        "L_TAX			 double," +
        "L_RETURNFLAG	varchar(1)," +
        "L_LINESTATUS	varchar(1)," +
        "L_SHIPDATE		 varchar(50)," +
        "L_COMMITDATE	 varchar(50)," +
        "L_RECEIPTDATE	 varchar(50)," +
        "L_SHIPINSTRUCT	varchar(25)," +
        "L_SHIPMODE		varchar(10)," +
        "L_COMMENT		varchar(44))"; 
        // String sql = "CREATE TABLE NATION (" +
        // "N_NATIONKEY		int," +
        // "N_NAME			varchar(25)," +
        // "N_REGIONKEY		long,  " +
        // "N_COMMENT		varchar(152))"; 

        // String sql = "CREATE TABLE REGION (" +
        // "R_REGIONKEY	int," +
        // "R_NAME		varchar(25)," +
        // "R_COMMENT	varchar(152))"; 
        
        
        
        String csvFilePath = "/Users/yiminglin/Documents/research/TPC/TPCH/2/";
        String tableName = "lineitem";
        CSVReader csvReader = new CSVReader();
        csvReader.loadTable(sql,tableName,csvFilePath);
    }

    
    public static int nextGaussian(Random r, int mean, int deviation, int min, int max){
        double next = r.nextGaussian()*deviation+mean;
        if(next > max){
            next = max;
        }
        if(next < min){
            next = min;
        }
        return (int)next;
    }

    public static HashMap<Integer, String> readStudentQueryTest(){
        String csvFile = "/Users/yiminglin/Documents/Codebase/datahub/filterplus/queries/query_student.txt";
        HashMap<Integer, String> queries = new HashMap<>();
        try{
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String sql = "";
            int queryID = 0;
            while((line = br.readLine()) != null) {
                System.out.println(line);
                if(line.charAt(0) == '#'){//end of a query
                    //System.out.println(queryID + " " + sql);
                    queries.put(queryID, sql);
                    sql = "";
                }else if(line.charAt(0) == 'Q'){//start of a new query
                    //System.out.println(line.charAt(1));
                    queryID = Character.getNumericValue(line.charAt(1));
                }else{
                    sql += line;
                    sql += " ";
                }
                //System.out.println(queryID + " " + sql);
            }
            br.close();
        }catch(IOException ioe) {
               ioe.printStackTrace();
        }
        for(Map.Entry<Integer,String> iter : queries.entrySet()){
            System.out.println(iter.getKey() + " " + iter.getValue());
        }
        return queries;
    }

    public static void runStudentQueries(String query){
        //System.out.print(query);
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
        Planner planner = VanillaDb.newPlanner();
        Plan plan = planner.createQueryPlan(query, tx);
        Scan s = plan.open();
        s.beforeFirst();
        while(s.next()){
            System.out.println(s.getVal("studentid"));//countofgradyear, avgofyearoffered, maxofstudentid, maxofgradyear
        }
        s.close();
        tx.commit();
    }

    public static void explainQuery(String query){
        query = "explain " + query;
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
        Planner planner = VanillaDb.newPlanner();
        Plan plan = planner.createQueryPlan(query, tx);
        
        Scan s = plan.open();
        s.beforeFirst();
        while(s.next()){
            System.out.println(s.getVal("query-plan"));
        }
        s.close();
        tx.commit();
    }
    public static void main(String[] args) {
        HashMap<Integer, String> studentQueries = readStudentQueryTest();
        String dbname = "TESTDB2";
        init(dbname);
        JoinKnob.forceHashJoin();
        //JoinKnob.disableProductJoin();
        //filterPlan.enable();
        //loadData();
        //createIndexByCode("student","sid");
        System.out.println("start running query...");
        long start = System.currentTimeMillis();
        runStudentQueries(studentQueries.get(4));
        explainQuery(studentQueries.get(4));
        long end = System.currentTimeMillis();
        System.out.println("running time: " + (end-start));
        System.out.println("number of dropped tupels: " + filterPlan.numberOfDroppedTuple);
        System.out.println("Filters:");
        filterPlan.printFilter();
        //resetDb(dbname);
        
        //test1();
        //testReadCSV();
        //test();
        //testCreateTable();
        //testInsert();
        //deleteIndex("idx_sid", "student", "sid");
        //createIndex("student","sid");
        //deleteIndex("idx_sid", "student", "sid");
        //testJoinSQL();
        //testTableSQL();

        //testCreateTableCode();
    }
}
