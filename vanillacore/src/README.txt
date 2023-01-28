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

Bug history:

Reason of bug: 

while (getLevelFlag(currentPage) > 0) {
            // read child block
            childBlk = new BlockId(currentPage.currentBlk().fileName(), childBlkNum);
            ccMgr.crabDownDirBlockForRead(childBlk);
            BTreePage child = new BTreePage(childBlk, NUM_FLAGS, schema, tx);

            // release parent block
            ccMgr.crabBackDirBlockForRead(parentBlk);
            currentPage.close();

            // move current block to child block
            currentPage = child;
            childBlkNum = findChildBlockNumber(searchKey);
            parentBlk = currentPage.currentBlk();
        }

Dead loop in Btree operation
Solved solution: change the student number to be 40000 -- solved, bug inside codebase 


-- optimization
TablePlanner is key part of optimization which decides which join to use, in function makeJoinPlan
1. currently hash join is not used, and multi-buffer product plan is used. Hash join cannot support theta join, so add filters for multi-buffer join and index join for now 

-- check filter modification

1. SelectScan
2. product scan/MultiBufferProductScan
3. indexjoinscan 

