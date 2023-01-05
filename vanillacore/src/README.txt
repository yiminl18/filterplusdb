-- schema
schema.toString(): return the current schema of returned result 

-- explain query plan 
String query = "explain select sname, gradyear, did from student, dept where majorid > did and sid>500 and gradyear < 2000";
        Plan plan = planner.createQueryPlan(query, tx);
        Scan s = plan.open();
        s.beforeFirst();
        while(s.next()){
            System.out.println(s.getVal("query-plan"));
            //System.out.println(s.getVal("sname") + " " + s.getVal("gradyear") + " " + s.getVal("did"));
        }

-- query execution 
- Plan() and Scan() are together for each implementation of operator, plan is for optimizer and scan is for executor 

-- query optimizer 
- blocksAccessed() in each class is used for optimization purpose -- estimate the number of blockes accesed by this operator 
- histogram in each class is used for optimization 

-- implementation types
- index: index based approaches, including index-based join and select
- materialize: blocking-operator, such as sort-merge join
- multibuffer: non-blocking-operator, such as hash join 

-- to do 

- figure out how to insert data one time and continously run queries 
