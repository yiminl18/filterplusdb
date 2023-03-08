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
import org.vanilladb.core.query.algebra.multibuffer.HashTables;
import org.vanilladb.core.query.planner.*;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.util.CSVReader;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.GlobalInfo;
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
import java.util.concurrent.*;

public class FullTest {

    // Flags
	//private static final BlockId FLAG_DATA_BLOCK = new BlockId("testing_flags", 0);
	//private static final int LOADED_FLAG_POS = 0;
    private static final String TPCHQUERY = "/Users/yiminglin/Documents/Codebase/filter_optimization/queries/tpch/tpch.txt";
    private static final String TPCHQUERYSERVER = "/home/yiminl18/filterOP/queries/tpch/tpch.txt";
    private static final String TPCHDATASERVER = "/home/yiminl18/filterOP/data/tpch/";
    private static final String TPCHDATA = "/Users/yiminglin/Documents/research/TPC/TPCH/2/";
    private static final String STUDENTQUERY = "/Users/yiminglin/Documents/Codebase/datahub/filterplus/queries/query_student.txt";
	private static final Constant DATA_LOADED_VALUE = new IntegerConstant(1);
    private static String dataOut = "tpc_time_server.txt";
    private static String resultOut = "tpc_result_server.txt";
    private static String planOut = "tpc_plan_server.txt";
    
    private static String queryIn = STUDENTQUERY;
    private static String dataIn = TPCHDATA;
    private static boolean writeKnob = true;

    public static void init(String dbname){
        ServerInit.init(dbname);
    }

    public static void loadData(){
        ServerInit.loadTestbed();
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
		//setFlagAsLoaded();
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

    public static void createTPCH(){
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
        
        
        
        String csvFilePath = dataIn;

        String tableName = "";
        List<String> fldNames = new ArrayList<>();
        String fldName = "";

        //create NATION
        System.out.println("Populating Nation...");
        tableName = "NATION";
        fldNames = new ArrayList<>();
        fldName = "N_NATIONKEY";
        fldNames.add(fldName.toLowerCase());
        CSVReader csvReader = new CSVReader();
        //csvReader.loadTable(sqlNATION,tableName.toLowerCase(),csvFilePath,fldNames);

        //create REGION
        System.out.println("Populating REGION...");
        tableName = "REGION";
        fldNames = new ArrayList<>();
        fldName = "R_REGIONKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        //csvReader.loadTable(sqlREGION,tableName.toLowerCase(),csvFilePath,fldNames);

        //create PART
        System.out.println("Populating PART...");
        tableName = "PART";
        fldNames = new ArrayList<>();
        fldName = "P_PARTKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        //csvReader.loadTable(sqlPART,tableName.toLowerCase(),csvFilePath,fldNames);

        //create SUPPLIER
        System.out.println("Populating SUPPLIER...");
        tableName = "SUPPLIER";
        fldNames = new ArrayList<>();
        fldName = "S_SUPPKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        //csvReader.loadTable(sqlSUPPLIER,tableName.toLowerCase(),csvFilePath,fldNames);

        //create PARTSUPP
        System.out.println("Populating PARTSUPP...");
        tableName = "PARTSUPP";
        fldNames = new ArrayList<>();
        fldName = "PS_PARTKEY";
        fldNames.add(fldName.toLowerCase());
        fldName = "PS_SUPPKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        //csvReader.loadTable(sqlPARTSUPP,tableName.toLowerCase(),csvFilePath,fldNames);

        //create CUSTOMER
        System.out.println("Populating CUSTOMER...");
        tableName = "CUSTOMER";
        fldNames = new ArrayList<>();
        fldName = "C_CUSTKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        //csvReader.loadTable(sqlCUSTOMER,tableName.toLowerCase(),csvFilePath,fldNames);

        //create ORDERS
        System.out.println("Populating ORDERS...");
        tableName = "ORDERS";
        fldNames = new ArrayList<>();
        fldName = "O_ORDERKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        //csvReader.loadTable(sqlORDERS,tableName.toLowerCase(),csvFilePath,fldNames);

        //create LINEITEM
        System.out.println("Populating LINEITEM...");
        tableName = "LINEITEM";
        fldNames = new ArrayList<>();
        fldName = "L_ORDERKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlLINEITEM,tableName.toLowerCase(),csvFilePath,fldNames);
        
    }


    public static HashMap<String, String> readQueryTest(){
        String csvFile = queryIn;
        HashMap<String, String> queries = new HashMap<>();
        try{
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String sql = "";
            String queryID = "";
            while((line = br.readLine()) != null) {
                //System.out.println(line);
                if(line.charAt(0) == '#'){//end of a query
                    //System.out.println(queryID + " " + sql);
                    queries.put(queryID, sql);
                    sql = "";
                }else if(line.charAt(0) == 'Q'){//start of a new query
                    //System.out.println(line.charAt(1));
                    queryID = line;
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

    public static String runQueries(String query){
        //System.out.println(query);
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
        Planner planner = VanillaDb.newPlanner();
        Plan plan = planner.createQueryPlan(query, tx);
        Scan s = plan.open();
        s.beforeFirst();
        List<String> projection = getProjection(query);
        System.out.println("Query answer: ");
        String out = "";
        while(s.next()){
            for(int i=0;i<projection.size();i++){
                System.out.print(s.getVal(projection.get(i)) + " ");
                out += s.getVal(projection.get(i)) + " ";
            }
            out += "\n";
            System.out.println("");
        }
        s.close();
        tx.commit();
        return out;
    }

    public static List<String> getProjection(String query){
        String[] projection = query.split(" ");
        String word = projection[1];
        //work for aggregate for now
        String[] attrs = word.split(",");
        List<String> output = new ArrayList<>();
        
        for(int i=0;i<attrs.length;i++){
            if(attrs[i].contains("(")){
                output.add(attrs[0].substring(0,attrs[i].length()-1).replace("(", "of"));
            }else{
                output.add(attrs[i]);
            }
        }
        // for(int i=0;i<output.size();i++){
        //     System.out.println(output.get(i));
        // }
        return output;
    }

    public static String explainQuery(String query){
        query = "explain " + query;
        Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
        Planner planner = VanillaDb.newPlanner();
        Plan plan = planner.createQueryPlan(query, tx);
        String queryPlan = "";
        Scan s = plan.open();
        s.beforeFirst();
        while(s.next()){
            System.out.println(s.getVal("query-plan"));
            queryPlan += s.getVal("query-plan");
            queryPlan += "\n";
        }
        s.close();
        tx.commit();
        return queryPlan;
    }

    public static void parseQuery(){
        String qry = "select student_name, count(distinct score), avg(sname), "
        + "sum(sid) from student, dept where sid=555 and sdid = did "
        + "group by student_name order by sid asc";

        Parser parser = new Parser(qry);
        QueryData data = parser.queryCommand();
        
        System.out.println(data.pred().toString()); 
    }

    public static long rawRun(String query, String queryID){
        //raw query run
        //System.out.println(query);
        HashTables.init();
        filterPlan.close();
        filterPlan.enableGroup = false;
        JoinKnob.init();
        JoinKnob.enableRawRun();
        long start = System.currentTimeMillis();
        String result = runQueries(query);
        long end = System.currentTimeMillis();
        long runTime = (end-start);
        
        System.out.println("Raw query run: " + runTime);
        //explainQuery(query);
        writeFile("Query " + queryID, resultOut);
        //writeFile(result, resultOut);
        return runTime;
    }

    private static long learnTime;

    public static long optimizedRun(String query, String queryID, boolean groupFilter){
        long start, end, runTime=0;
        //run optimized query
        HashTables.init();
        filterPlan.init();
        filterPlan.open();
        JoinKnob.init();

        // fast learning phase
        start = System.currentTimeMillis();
        JoinKnob.enableFastLearning();
        runQueries(query);
        end = System.currentTimeMillis();
        learnTime = (end-start);
        filterPlan.printFilter();
        String newQ = filterPlan.mergePredicate(query);
        System.out.println(newQ);
        explainQuery(query);

        //query run phase
        // filterPlan.open();
        // start = System.currentTimeMillis();

        // JoinKnob.init();//close fast learning

        // filterPlan.enableGroup = groupFilter;
        // runQueries(newQ);

        // end = System.currentTimeMillis();
        // runTime = (end-start);
        
        // System.out.println(learnTime + " " + runTime);
        // filterPlan.printFilter();
        // filterPlan.filterStats();

        // if(groupFilter){
        //     String p = explainQuery(newQ);
        //     writeFile(p, planOut);
        // }
        

        return runTime;
    }

    public static long OptimizeRunNoLearning(String query, String queryID, boolean groupFilter){
        long start, end, runTime;
        filterPlan.init();
        filterPlan.open();
        HashTables.init();
        JoinKnob.init();
        JoinKnob.rawRun = true;
        start = System.currentTimeMillis();
        filterPlan.enableGroup = groupFilter;

        String result = runQueries(query);

        end = System.currentTimeMillis();
        runTime = (end-start);

        filterPlan.filterStats();
        filterPlan.printFilter();

        if(groupFilter){
            writeFile("Query " + queryID, resultOut);
            writeFile(result, resultOut);
        }

        // if(groupFilter){
        //     filterPlan.init();
        //     filterPlan.open();
        //     HashTables.init();
        //     JoinKnob.init();
        //     JoinKnob.rawRun = true;
        //     String p = explainQuery(query);
        //     writeFile(p, planOut);
        // }

        System.out.println(runTime);

        return runTime;
    }

    public static String oneRun(String query, String queryID){
        System.out.println("Query " + queryID);
        System.out.println(query);
        writeFile("Query " + queryID, planOut);

        //Raw query run
        long rawRunTime = 0;
        rawRunTime = rawRun(query, queryID);

        writeFile("Query " + queryID, dataOut); 
        String out = "Raw query run: "; 
        writeFile(out, dataOut);
        String out1 = "run time: " + String.valueOf(rawRunTime);
        writeFile(out1, dataOut);

        //Optimized query run without learning

        //System.out.println(out + " " + out1); 
        long opTimeNoLearningbest = 0;
        long opTimeNoLearning = 0;
        long opTimeNoLearningNoGroup = 0;

        if(query.contains("group by")){
            opTimeNoLearning = OptimizeRunNoLearning(query, queryID, true);
            opTimeNoLearningNoGroup = OptimizeRunNoLearning(query, queryID, false);
            
            if(opTimeNoLearning < opTimeNoLearningNoGroup){//with group filter is better
                opTimeNoLearningbest = opTimeNoLearning;
            }else{//without group filter is better 
                opTimeNoLearningbest = opTimeNoLearningNoGroup;
            }
        }else{
            opTimeNoLearningbest = OptimizeRunNoLearning(query, queryID, true);
        }

        out = "Optimized query run: ";
        writeFile(out, dataOut);
        out1 = "run time: " + String.valueOf(opTimeNoLearningbest);
        writeFile(out1, dataOut);

        //System.out.println(out + " " + out1);
        
        // //Optimized query run with learning
        // if(query.contains("group by")){
        //     if(opTimeNoLearning < opTimeNoLearningNoGroup){//with group filter is better
        //         opTime = optimizedRun(query, queryID, true);
        //     }else{//without group filter is better 
        //         opTime = optimizedRun(query, queryID, false);
        //     }
        // }else{
        //     opTime = optimizedRun(query, queryID, true);
        // }
        
        // out = "Optimized query run: ";
        // writeFile(out, dataOut);
        // out1 = "learn time: " + String.valueOf(learnTime);
        // writeFile(out1, dataOut);
        // String out2 = "run time: " + String.valueOf(opTime);
        // writeFile(out2, dataOut);

        //System.out.println(out + " " + out1 + " " + out2);
        return "";
    }

    public static void writeFile(String line, String fileName){
        if(!writeKnob){
            return;
        }
        File file = new File(fileName); 
        try {
            FileWriter out = new FileWriter(file, true);
            BufferedWriter bw=new BufferedWriter(out);

            bw.write(line);
            bw.newLine();

            bw.flush();
            bw.close();
            }catch (IOException e) {e.printStackTrace();}
    }

    public static String check(){
        try{
            Thread.sleep(3000);
        }catch(InterruptedException e){};
        return "6";
    }

    public static void timeChecker(long time, String query, String queryID){
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(()->{
            String result = oneRun(query, queryID);
            return result;
        });

        try{
            String result = future.get(time, TimeUnit.SECONDS);
            //System.out.println(result);
        }catch(TimeoutException e){
            System.out.println("Timeout!");
            writeFile("Timeout for " + String.valueOf(time) + " seconds limit", dataOut);
            writeFile("Timeout for " + String.valueOf(time) + " seconds limit", planOut);
        }catch(ExecutionException e){
        }catch(InterruptedException e){
        }

        executor.shutdown();
    }

    public static void getAllQueriedAttrs(HashMap<String, String> Queries){
        for (Map.Entry<String, String> entry : Queries.entrySet()) {
            String query = entry.getValue();
            Parser parser = new Parser(query);
            QueryData data = parser.queryCommand();
            GlobalInfo.getqueriedAttrAll(data);
        }
        GlobalInfo.print();
    }

    public void testSplit(){
        String a = "xxx|35";
        String[] words = a.split("\\|");
        System.out.println(words[0] + " " + words[1]);
    }

    @Test
    public void main() {
        //testSplit();
        HashMap<String, String> Queries = readQueryTest();
        getAllQueriedAttrs(Queries);
        String dbname = "TESTDB2";//TESTDB2
        long start = System.currentTimeMillis();
        init(dbname);
        long end = System.currentTimeMillis();
        System.out.println(end-start);
        //parseQuery();
        //createTPCH();
        //testReadCSV();
        writeKnob = false;

        // for (Map.Entry<String, String> entry : Queries.entrySet()) {
        //     String queryID = entry.getKey();
        //     timeChecker(5,entry.getValue(), queryID);
        // }

        String queryID = "Q2";

        oneRun(Queries.get(queryID), queryID);
    }
}
