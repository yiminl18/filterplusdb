package org.vanilladb.core;

import java.sql.Connection;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.query.planner.*;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.sql.*;
import org.vanilladb.core.storage.metadata.*;

public class testrun {

    public static void init(){
        ServerInit.init(BasicQueryTest.class);
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
        String query = "select sid, sname, gradyear from student where sid>500 and gradyear < 2000";
        Plan plan = planner.createQueryPlan(query, tx);
        Scan s = plan.open();
        s.beforeFirst();
        while(s.next()){
            System.out.println(s.getVal("sid") + " " + s.getVal("sname").asJavaVal() + " " + s.getVal("gradyear"));
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
            //System.out.println(s.getVal("query-plan"));
            System.out.println(s.getVal("sname") + " " + s.getVal("gradyear") + " " + s.getVal("did"));
        }
        s.close();
        tx.commit();
    }

    
    public static void main(String[] args) {
        init();
        testJoinSQL();
    }
}
