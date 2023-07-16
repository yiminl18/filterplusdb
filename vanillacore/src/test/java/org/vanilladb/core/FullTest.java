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
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.util.dataProcessing;
import org.w3c.dom.css.CSSStyleDeclaration;
public class FullTest {

    // Flags
	//private static final BlockId FLAG_DATA_BLOCK = new BlockId("testing_flags", 0);
	//private static final int LOADED_FLAG_POS = 0;
    private static final String TPCHQUERY = "/Users/yiminglin/Documents/Codebase/filter_optimization/queries/tpch/tpch1.txt";
    private static final String TPCHQUERYSERVER = "/home/yiminl18/filterOP/queries/tpch/tpch.txt";
    private static final String TPCHDATASERVER = "/home/yiminl18/filterOP/data/tpch/";
    private static final String TPCHDATA = "/Users/yiminglin/Documents/research/TPC/TPCH/zipf2/";
    private static final String STUDENTQUERY = "/Users/yiminglin/Documents/Codebase/datahub/filterplus/queries/query_student.txt";
    private static final String SMARTBENCHDATA = "/Users/yiminglin/Documents/Codebase/filter_optimization/data/smartbench/";
    private static final String IMDBDATA = "/Users/yiminglin/Documents/research/Data/IMDB/sampled/";
    private static final String SMARTBENCHQUERY = "/Users/yiminglin/Documents/Codebase/filter_optimization/queries/smartbench/query1.txt";
    private static final String IMDBQUERY = "/Users/yiminglin/Documents/Codebase/filter_optimization/queries/imdb/query1.txt";
    private static final String TABLE = "/Users/yiminglin/Documents/Codebase/filter_optimization/scripts/IMDB/createtables.txt";
    private static String dataOut = "TPCH_time";
    private static String resultOut = "TPCH_result";
    private static String planOut = "TPCH_plan";
    
    private static String queryIn = TPCHQUERY;
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
        String line = "n_nationkey|n_name|n_regionkey|n_comment";
        String delimiter = "|";
        String[] header = line.split("\\|");
        for(int j = 0;j<header.length;j++){
            System.out.println(header[j]);
        }

        String str = "Hello,World,Java";
        String[] parts = str.split(",");

        System.out.println(parts.length);

        for (String part : parts) {
            System.out.println(part);
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

    public static void createSmartBench(){
        String sqlUSER = "CREATE TABLE users (" + 
            "users_mac		int," + 
            "users_name		int," + 
            "users_email	int," + 
            "users_ugroup	int)" ;
        String sqlWIFI = "CREATE TABLE wifi (" + 
        "wifi_st		int," + 
        "wifi_et		int," + 
        "wifi_mac	int," + 
        "wifi_lid	int," + 
        "wifi_duration	int)" ;
        String sqlOCCUPANCY = "CREATE TABLE occupancy (" + 
        "occupancy_lid		int," + 
        "occupancy_st		int," + 
        "occupancy_et	int," + 
        "occupancy_occupancy	int," + 
        "occupancy_type	int)" ;
        String sqlLOCATION = "CREATE TABLE location (" + 
        "location_lid		int," + 
        "location_building		int," + 
        "location_floor	int," + 
        "location_type	int," + 
        "location_capacity	int)" ;
        
        String csvFilePath = dataIn;

        String tableName = "";
        List<String> fldNames = new ArrayList<>();
        String fldName = "";

        //create USERs
        System.out.println("Populating USERs...");
        tableName = "users";
        fldNames = new ArrayList<>();
        fldName = "users_mac";
        fldNames.add(fldName.toLowerCase());
        CSVReader csvReader = new CSVReader();
        csvReader.loadTable(sqlUSER,tableName.toLowerCase(),csvFilePath,fldNames);

        //create WIFI
        System.out.println("Populating WIFI...");
        tableName = "wifi";
        fldNames = new ArrayList<>();
        csvReader = new CSVReader();
        csvReader.loadTable(sqlWIFI,tableName.toLowerCase(),csvFilePath,fldNames);

        //create OCCUPANCY
        System.out.println("Populating OCCUPANCY...");
        tableName = "occupancy";
        fldNames = new ArrayList<>();
        csvReader = new CSVReader();
        csvReader.loadTable(sqlOCCUPANCY,tableName.toLowerCase(),csvFilePath,fldNames);

        //create LOCATION
        System.out.println("Populating LOCATION...");
        tableName = "location";
        fldNames = new ArrayList<>();
        fldName = "location_lid";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlLOCATION,tableName.toLowerCase(),csvFilePath,fldNames);
    }

    public static void createTables(List<Table> tables){
        for(Table table: tables){
            CSVReader csvReader = new CSVReader();
            csvReader.loadTable(table.getSql(),table.getTbl(),dataIn,table.getIdx());
        }
    }

    public void readFile(){
        String csvFile = dataIn + "title.csv";
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String delimiter = ",";
            String[] values;
            line = br.readLine();
            String[] header = line.split(delimiter);
            int idx = 0;
            while((line = br.readLine()) != null) {
                idx += 1;
                if(idx > 10){
                    break;
                }
                values = line.split(delimiter);//values are a set of value in one tuple
                
                for(int i=0;i<values.length;i++){
                    if(values[i].equals("\\N")){
                        System.out.print("missing ");
                    }else{
                        System.out.print(values[i]+ " ");
                    }
                    
                }
                System.out.println("");
            }
            
            br.close();
            } catch(IOException ioe) {
               ioe.printStackTrace();
            }
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
        csvReader.loadTable(sqlNATION,tableName.toLowerCase(),csvFilePath,fldNames);

        //create REGION
        System.out.println("Populating REGION...");
        tableName = "REGION";
        fldNames = new ArrayList<>();
        fldName = "R_REGIONKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlREGION,tableName.toLowerCase(),csvFilePath,fldNames);

        //create PART
        System.out.println("Populating PART...");
        tableName = "PART";
        fldNames = new ArrayList<>();
        fldName = "P_PARTKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlPART,tableName.toLowerCase(),csvFilePath,fldNames);

        //create SUPPLIER
        System.out.println("Populating SUPPLIER...");
        tableName = "SUPPLIER";
        fldNames = new ArrayList<>();
        fldName = "S_SUPPKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlSUPPLIER,tableName.toLowerCase(),csvFilePath,fldNames);

        //create PARTSUPP
        System.out.println("Populating PARTSUPP...");
        tableName = "PARTSUPP";
        fldNames = new ArrayList<>();
        fldName = "PS_PARTKEY";
        fldNames.add(fldName.toLowerCase());
        fldName = "PS_SUPPKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlPARTSUPP,tableName.toLowerCase(),csvFilePath,fldNames);

        //create CUSTOMER
        System.out.println("Populating CUSTOMER...");
        tableName = "CUSTOMER";
        fldNames = new ArrayList<>();
        fldName = "C_CUSTKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlCUSTOMER,tableName.toLowerCase(),csvFilePath,fldNames);

        //create ORDERS
        System.out.println("Populating ORDERS...");
        tableName = "ORDERS";
        fldNames = new ArrayList<>();
        fldName = "O_ORDERKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlORDERS,tableName.toLowerCase(),csvFilePath,fldNames);

        //create LINEITEM
        System.out.println("Populating LINEITEM...");
        tableName = "LINEITEM";
        fldNames = new ArrayList<>();
        fldName = "L_ORDERKEY";
        fldNames.add(fldName.toLowerCase());
        csvReader = new CSVReader();
        csvReader.loadTable(sqlLINEITEM,tableName.toLowerCase(),csvFilePath,fldNames);
        
    }

    public static class Table{
        public String tbl;
        public List<String> idx;
        public String sql;
        public Table(String tbl, List<String> idx, String sql) {
            this.tbl = tbl;
            this.idx = idx;
            this.sql = sql;
        }
        public void setIdx(List<String> idx) {
            this.idx = idx;
        }
        
        public List<String> getIdx(){
            return idx;
        }

        public String getTbl() {
            return tbl;
        }
        public void setTbl(String tbl) {
            this.tbl = tbl;
        }
        public String getSql() {
            return sql;
        }
        public void setSql(String sql) {
            this.sql = sql;
        }
        public void printIdx(){
            for (String id : idx){
                System.out.print(id + " ");
            }
            System.out.println("");
        }
    }

    public static List<Table> readCreateTableSQL(){
        String csvFile = queryIn;
        List<Table> tables = new ArrayList<>();
        try{
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String sql = "";
            List<String> idx = new ArrayList<>();
            while((line = br.readLine()) != null) {
                //read tablename
                String tbl = line.trim();
                line = br.readLine();
                //read index
                int indexNum = Integer.valueOf(line);
                if(indexNum > 0){
                    int c = 0;
                    while(c<indexNum){
                        c += 1;
                        line = br.readLine();
                        idx.add(line.trim());
                    }
                }
                line = br.readLine();
                String keyword = line.substring(0, 6);
                if(keyword.equals("create")){
                    sql += line;
                    while((line = br.readLine()) != null) {
                        if(line.charAt(0) == '#'){
                            tables.add(new Table(tbl, idx, sql));
                            idx = new ArrayList<>();
                            sql = "";
                            tbl = "";
                            break;
                        }
                        sql += line;
                    }
                }
            }
            br.close();
        }catch(IOException ioe) {
               ioe.printStackTrace();
        }
        //printing
        // for(int i=0;i<tables.size();i++){
        //     Table table = tables.get(i);
        //     System.out.println(table.getTbl());
        //     table.printIdx();
        //     System.out.println(table.getSql());
        // }
        return tables;
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
                output.add(attrs[i].substring(0,attrs[i].length()-1).replace("(", "of"));
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
        HashTables.init();
        filterPlan.init();
        JoinKnob.init();
        JoinKnob.enableRawRun();
        long start = System.currentTimeMillis();
        String result = runQueries(query);
        long end = System.currentTimeMillis();
        long runTime = (end-start);
        
        System.out.println("Raw query run: " + runTime);
        writeFile("Query " + queryID, resultOut);
        writeFile(result, resultOut);
        String filterStats = filterPlan.filterStats();
        filterPlan.printFilter();
        System.out.println(filterStats);
        writeFile("Raw query run:", planOut);
        writeFile(filterStats, planOut);

        filterPlan.filterBuilding = false;
        //explainQuery(query);
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
        //explainQuery(query);

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
        filterPlan.filterStats();
        HashTables.init();
        JoinKnob.init();
        JoinKnob.rawRun = true;
        start = System.currentTimeMillis();
        filterPlan.enableGroup = groupFilter;

        String result = runQueries(query);

        end = System.currentTimeMillis();
        runTime = (end-start);

        String filterStats = filterPlan.filterStats();
        //filterPlan.printFilter();

        if(groupFilter){
            writeFile("Query " + queryID, resultOut);
            writeFile(result, resultOut);
        }

        //System.out.println("Optimized query run time: " + runTime);
        filterPlan.printFilter();
        

        System.out.println(filterStats);
        writeFile("Optimized query run:", planOut);
        writeFile(filterStats, planOut);

        filterPlan.filterBuilding = false;
        //explainQuery(query);

        return runTime;
    }

    public static String oneRun(String query, String queryID){
        System.out.println("Query " + queryID);
        System.out.println("");
        writeFile("Query " + queryID, dataOut);
        //System.out.println(query);
        String out = "", out1 = "";

        


        //Raw query run
        long rawRunTime = 0;
        rawRunTime = rawRun(query, queryID);
        out = "Raw query run: "; 
        writeFile(out, dataOut);
        out1 = "run time: " + String.valueOf(rawRunTime);
        writeFile(out1, dataOut);

        //Optimized query run without learning

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

        System.out.println(out + " " + out1);
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

    public void testProperty(){
        String path = System.getProperty("org.vanilladb.core.config.file");
        System.out.println("**** " + path);
        int val = CoreProperties.getLoader().getPropertyAsInteger(
				"org.vanilladb.core.storage.buffer.BufferMgr.BUFFER_POOL_SIZE", 20);
        System.out.println(val);
    }

    public void checkDir(String dirName){
        File folder = new File(dirName);
        if (!folder.isDirectory()) {
            folder.mkdir();
        }
    }

    public void cleanFiles(){
        File file = new File(dataOut);
        if(file.exists()){
            file.delete();
        }
        file = new File(resultOut);
        if(file.exists()){
            file.delete();
        }
    }

    

    @Test
    public void main() {
        //test();
        //first create dataset, and then delete histogram folder, then run init again 
        testProperty();
        HashMap<String, String> Queries = readQueryTest();
        getAllQueriedAttrs(Queries);
        String dbname = "TPCHSF1";//TPCHSF1|IMDB|SmartBench|TPCHZIPF1|TPCHZIPF2
        GlobalInfo.setHistogramPath(dbname);
        init(dbname);
        //createTPCH();
        //createTables(readCreateTableSQL());
        // //createSmartBench();
        // // String version = "1";
        // // dataOut = dataOut + "_" + dbname + "_" + version + ".txt";
        // // resultOut = resultOut + "_" + dbname + "_" + version + ".txt";
        // // planOut = planOut + "_" + dbname + "_" + version + ".txt";
        // // // //cleanFiles();
        //writeKnob = false;

        


        String queryID = "Q23";

        oneRun(Queries.get(queryID), queryID);

        // for (Map.Entry<String, String> entry : Queries.entrySet()) {
        //     String queryID = entry.getKey();
        //     timeChecker(50,entry.getValue(), queryID);
        // }
    }
}
