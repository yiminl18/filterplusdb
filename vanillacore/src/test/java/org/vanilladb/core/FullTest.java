package org.vanilladb.core;

import java.sql.Connection;
import java.util.*;
import java.io.File;
import java.io.*;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.query.parse.*;
import org.vanilladb.core.filter.filter;
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
import org.junit.jupiter.api.Test;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException; 

public class FullTest {

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
        String sqlPART = "CREATE TABLE PART (" + 
            "P_PARTKEY		int," + 
            "P_NAME			varchar(55)," + 
            "P_MFGR			varchar(25)," + 
            "P_BRAND			varchar(10)," + 
            "P_TYPE			varchar(25)," + 
            "P_SIZE			int," + 
            "P_CONTAINER		varchar(10)," + 
            "P_RETAILPRICE	double," + 
            "P_COMMENT		varchar(23))" ;
        String sqlSUPPLIER = "CREATE TABLE SUPPLIER (" + 
        "S_SUPPKEY		int," + 
        "S_NAME			varchar(25)," + 
        "S_ADDRESS		varchar(40)," + 
        "S_NATIONKEY		long, " + 
        "S_PHONE			varchar(15)," + 
        "S_ACCTBAL		double," + 
        "S_COMMENT		varchar(101))"; 
        String sqlPARTSUPP = "CREATE TABLE PARTSUPP (" + 
        "PS_PARTKEY		long  , " + 
        "PS_SUPPKEY		long  , " + 
        "PS_AVAILQTY		int," + 
        "PS_SUPPLYCOST	 double," + 
        "PS_COMMENT		varchar(199))";
        String sqlCUSTOMER = "CREATE TABLE CUSTOMER (" + 
        "C_CUSTKEY		int," + 
        "C_NAME			varchar(25)," + 
        "C_ADDRESS		varchar(40)," + 
        "C_NATIONKEY		long, " + 
        "C_PHONE			varchar(15)," + 
        "C_ACCTBAL		 double," + 
        "C_MKTSEGMENT	varchar(10)," + 
        "C_COMMENT		varchar(117))";
        String sqlORDERS = "CREATE TABLE ORDERS (" +
        "O_ORDERKEY		int," +
        "O_CUSTKEY		long," +
        "O_ORDERSTATUS	varchar(1)," +
        "O_TOTALPRICE	 double," +
        "O_ORDERDATE		 varchar(50)," +
        "O_ORDERPRIORITY	varchar(15)," +
        "O_CLERK			varchar(15)," +
        "O_SHIPPRIORITY	int," +
        "O_COMMENT		varchar(79))";
        String sqlLINEITEM = "CREATE TABLE LINEITEM (" +
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
        String sqlNATION = "CREATE TABLE NATION (" +
        "N_NATIONKEY		int," +
        "N_NAME			varchar(25)," +
        "N_REGIONKEY		long,  " +
        "N_COMMENT		varchar(152))"; 

        String sqlREGION = "CREATE TABLE REGION (" +
        "R_REGIONKEY	int," +
        "R_NAME		varchar(25)," +
        "R_COMMENT	varchar(152))"; 
        
        
        
        String csvFilePath = "/Users/yiminglin/Documents/research/TPC/TPCH/2/";

        String tableName = "";
        List<String> fldNames = new ArrayList<>();
        String fldName = "";

        //create PART
        tableName = "PART";
        fldNames = new ArrayList<>();
        fldName = "P_PARTKEY";
        fldNames.add(fldName.toLowerCase());
        CSVReader csvReader = new CSVReader();
        csvReader.loadTable(sqlPART,tableName.toLowerCase(),csvFilePath,fldNames);

        //create SUPPLIER
        tableName = "SUPPLIER";
        fldNames = new ArrayList<>();
        fldName = "S_SUPPKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlSUPPLIER,tableName.toLowerCase(),csvFilePath,fldNames);

        //create PARTSUPP
        tableName = "PARTSUPP";
        fldNames = new ArrayList<>();
        fldName = "PS_PARTKEY";
        fldNames.add(fldName.toLowerCase());
        fldName = "PS_SUPPKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlPARTSUPP,tableName.toLowerCase(),csvFilePath,fldNames);

        //create CUSTOMER
        tableName = "CUSTOMER";
        fldNames = new ArrayList<>();
        fldName = "C_CUSTKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlCUSTOMER,tableName.toLowerCase(),csvFilePath,fldNames);

        //create ORDERS
        tableName = "ORDERS";
        fldNames = new ArrayList<>();
        fldName = "O_ORDERKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlORDERS,tableName.toLowerCase(),csvFilePath,fldNames);

        //create LINEITEM
        tableName = "LINEITEM";
        fldNames = new ArrayList<>();
        fldName = "L_ORDERKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlLINEITEM,tableName.toLowerCase(),csvFilePath,fldNames);

        //create NATION
        tableName = "NATION";
        fldNames = new ArrayList<>();
        fldName = "N_NATIONKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlNATION,tableName.toLowerCase(),csvFilePath,fldNames);

        //create REGION
        tableName = "REGION";
        fldNames = new ArrayList<>();
        fldName = "R_REGIONKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlREGION,tableName.toLowerCase(),csvFilePath,fldNames);
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
                //System.out.println(line);
                if(line.charAt(0) == '#'){//end of a query
                    //System.out.println(queryID + " " + sql);
                    queries.put(queryID, sql);
                    sql = "";
                }else if(line.charAt(0) == 'Q'){//start of a new query
                    //System.out.println(line.charAt(1));
                    queryID = Integer.valueOf(line.substring(1, line.length()));
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
        // for(Map.Entry<Integer,String> iter : queries.entrySet()){
        //     System.out.println(iter.getKey() + " " + iter.getValue());
        // }
        return queries;
    }

    public static void runStudentQueries(String query){
        //System.out.println(query);
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
        Planner planner = VanillaDb.newPlanner();
        Plan plan = planner.createQueryPlan(query, tx);
        Scan s = plan.open();
        s.beforeFirst();
        List<String> projection = getProjection(query);
        // for(int i=0;i<projection.size();i++){
        //     System.out.println(projection.get(i) + " ");
        // }
        while(s.next()){
            for(int i=0;i<projection.size();i++){
                System.out.print(s.getVal(projection.get(i)) + " ");
            }
            System.out.println("");
        }
        s.close();
        tx.commit();
    }

    public static List<String> getProjection(String query){
        String[] projection = query.split(" ");
        String word = projection[1];
        //work for aggregate for now
        String[] attrs = word.split(",");
        List<String> output = new ArrayList<>();
        output.add(attrs[0].substring(0,attrs[0].length()-1).replace("(", "of"));
        for(int i=1;i<attrs.length;i++){
            output.add(attrs[i]);
        }
        // for(int i=0;i<output.size();i++){
        //     System.out.println(output.get(i));
        // }
        return output;
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

    public static void parseQuery(){
        String qry = "select student_name, count(distinct score), avg(sname), "
        + "sum(sid) from student, dept where sid=555 and sdid = did "
        + "group by student_name order by sid asc";

        Parser parser = new Parser(qry);
        QueryData data = parser.queryCommand();
        
        System.out.println(data.pred().toString()); 
    }



    public static void oneRun(String query, int queryID){
        //raw query run
        filterPlan.close();
        JoinKnob.init();
        JoinKnob.enableRawRun();
        long start = System.currentTimeMillis();
        JoinKnob.disableHashJoin();
        JoinKnob.disableIndexJoin();
        JoinKnob.disableNestLoopJoin();
        //runStudentQueries(query);
        long end = System.currentTimeMillis();
        long runTime = (end-start);
        String out2 = "Raw query run: " + String.valueOf(runTime); 
        //System.out.println(runTime);

        System.out.println(query);

        //run optimized query
        filterPlan.init();
        filterPlan.open();
        JoinKnob.init();

        //fast learning phase
        start = System.currentTimeMillis();
        JoinKnob.enableFastLearning();
        runStudentQueries(query);
        end = System.currentTimeMillis();
        long learnTime = (end-start);
        filterPlan.printFilter();
        String newQ = filterPlan.mergePredicate(query);

        System.out.println(newQ);

        //query run phase
        // filterPlan.open();
        // start = System.currentTimeMillis();
        // JoinKnob.init();//close fast learning
        // JoinKnob.disableHashJoin();
        // JoinKnob.disableIndexJoin();
        // JoinKnob.disableNestLoopJoin();
        // runStudentQueries(newQ);
        // end = System.currentTimeMillis();
        // runTime = (end-start);
        // String out1 = "Optimized query run: " + String.valueOf(learnTime) + " " + String.valueOf(runTime);
        // System.out.println(learnTime + " " + runTime);
        // explainQuery(query);
        // filterPlan.printFilter();

        // writeFile(out1, out2, queryID);
    }

    public static void writeFile(String line1, String line2, int queryID){
        File file = new File("output.txt"); 
        try {
            FileWriter out = new FileWriter(file, true);
            BufferedWriter bw=new BufferedWriter(out);

            bw.write(queryID);
            bw.newLine();
            bw.write(line1);
            bw.newLine();
            bw.write(line2);
            bw.newLine();

            bw.flush();
            bw.close();
            }catch (IOException e) {e.printStackTrace();}
    }

    @Test
    public void main() {
        HashMap<Integer, String> studentQueries = readStudentQueryTest();
        // getProjection(studentQueries.get(11));
        String dbname = "TESTDB2";//TESTDB2
        init(dbname);
        //parseQuery();
        //loadData();
        //testReadCSV();
        int queryID = 3;
        oneRun(studentQueries.get(queryID), queryID);

    }
}
