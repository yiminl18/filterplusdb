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

-- The way to print aggreagte value!

s.getVal("countofgradyear”)
sumofsid
avgofsname
dstcountofscore
minof+attributename


-index codes:

1. the following code only creates the index entry in metadata and stored in idxcat file, do not populate the index records data 
indexedFlds.add(fieldName);
String idxName = "idx_" + fieldName;
md.createIndex(idxName, tbName, indexedFlds, IndexType.BTREE, tx);

2. the following code populate index records data when insert records into tables 

if(is_index){
					fields = new ArrayList<>();
					fields.add("sid");
					fields.add("sname");
					fields.add("majorid");
					fields.add("gradyear");
					vals = new ArrayList<>();
					vals.add(sid);
					vals.add(sname);
					vals.add(majorid);
					vals.add(gradyear);
					InsertData data = new InsertData(tbName, fields,vals);
					new IndexUpdatePlanner().executeInsert(data, tx);
				}
3. See loadTestbed in ServerInit to insert data and index 
First create table, then index, and finally insert records both to table and index using IndexUpdatePlanner



-- optimization
TablePlanner is key part of optimization which decides which join to use, in function makeJoinPlan
1. currently hash join is not used, and multi-buffer product plan is used. Hash join cannot support theta join, so add filters for multi-buffer join and index join for now 
 

-- TableScan
returns a RecordFile instead of a record 

-- IndexSelect 
IndexSelectScan(Index idx, SearchRange searchRange, TableScan ts)
the searchRange is computed by the select predicate as expected
- the action in next(): set the tuple id returned by a tule from index, so that the tuple can be retrieved from the tablescan  
	ts.moveToRecordId(rid) moves the tuple id to tablescan to be retrived 
- output is a Scan - Plan 

-- SelectScan
SelectPlan is put everywhere in the tree as a filter
the input is Plan, which is a Scan method, and next() returns each tuple 
- A SelectScan that is put after a IndexSelect contains the selection predicate to filter each tuple 
	Q: how to get each tuple from the output of IndexSelect? -- use Scan as connector
	- The data flow between IndexSelect and SelectScan is pipeline, SelectScan calls get next(), this next() will call next() in IndexSelect 

-- IndexJoin 
The scenario that an IndexJoin will be preferred is R.a join S.b, a is the index of R and |R| > |S|
- input: left-hand plan -- Plan p1; right-hand plan --  TablePlan tp2 (IndexPlan)
- output: Scan, and next() returns each tuple 
this tuple is not the join result, but product result 
- data flow between IndexJoin and SelectScan: pipeline, each next() in SelectScan will call next() in IndexJoin 
- IndexJoin is able to add theta join filter point in next() or resetIndex function 

-MultiBufferProduct -- actually a nested loop join 
- input: Plan lhs, rhs
in the right hand side, TempTable tt = copyRecordsFrom(rhs)
- process:
1. materializes its RHS query
2. divide right side relation into each chunk
3. do left table join with each of right chunk 
In MultiBufferProductScan:
1. prodScan = new ProductScan(lhsScan, rhsScan), rhsScan is each chunk, lhsScan is left table, prodScan returns the product of left table and this chunk 
- output: return Scan, and next() returns each combined tuple which is the output of product 
- data flow: it is pipeline data flow between MultiBufferProductScan and SelectScan on top of it: a product tuple will be immidiately sent to SelectScan to evaluate 

-- Data flow between recordfile and record 
Each record.next() call recordfile.next(), and their connection is getVal()
1. getVal() in tableScan: gets value in the page by giving the pageid and offset 
2. In FieldNameExpression, evaluate(Record rec), return rec.getVal(fldName)
3. isSatisfied in scan.next() call evaluate in FieldNameExpression

