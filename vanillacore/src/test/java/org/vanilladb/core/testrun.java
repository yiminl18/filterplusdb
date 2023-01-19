package org.vanilladb.core;

import java.sql.Connection;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.query.planner.*;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.util.CSVReader;

public class testrun {

    public static void init(String dbname){
        ServerInit.init(dbname);
    }

    public static void loadData(){
        ServerInit.loadTestbed();
    }

    public static void resetDb(String dbname){
        ServerInit.resetDb(dbname);
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
        String query = "select a, b, c from test2";
        Plan plan = planner.createQueryPlan(query, tx);
        Scan s = plan.open();
        s.beforeFirst();
        while(s.next()){
            System.out.println(s.getVal("a") + " " + s.getVal("b").asJavaVal() + " " + s.getVal("c"));
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

    public static void test1(){
        System.out.println("hello");
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
        String sql = "create table test2(a int, b int, c int)";
        CSVReader csvReader = new CSVReader();
        csvReader.loadTable(sql,"test2");
    }

    public static void testCreateTableCode(){
        
    }

    public static void main(String[] args) {
        String dbname = "TPCH";
        //resetDb(dbname);
        init(dbname);
        //testReadCSV();
        //testCreateTable();
        //testInsert();
        //loadData();
        //testJoinSQL();
        testTableSQL();

        //testCreateTableCode();
    }
}
