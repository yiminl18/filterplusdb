package org.vanilladb.core.storage.metadata;

import java.util.*;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;

/*
 * This class is used to store the global information shared via multi-class. 
 */
public class GlobalInfo {
    public static List<String> queriedAttr; //stores projected attrs and attrs in query
    public static List<String> queriedAttrAll = new ArrayList<>();
    public static String histogramPath;

    public static void setHistogramPath(String path){
        histogramPath = path;
    }

    public List<String> getQueriedAttrAllValue(){
        return queriedAttrAll;
    }

    public static void getqueriedAttr(QueryData data){
        queriedAttr = new ArrayList<>();
        //add projected attrs
        for (String fldName : data.projectFields()) {
            queriedAttr.add(fldName);
        }
        //add aggregate attrs
        if (data.aggregationFn() != null)
			for (AggregationFn aggFn : data.aggregationFn()) {
				String aggFld = aggFn.argumentFieldName();
                if(!queriedAttr.contains(aggFld)){
                    queriedAttr.add(aggFld);
                }
			}
        //add group by attrs
        if (data.groupFields() != null)
        for (String groupByFld : data.groupFields()) {
            if(!queriedAttr.contains(groupByFld)){
                queriedAttr.add(groupByFld);
            }
        }
        //add predicate attrs
        for (Term term : data.pred().getTerms()){
            String attr = term.getLhsField();
            if(!attr.equals("null") && !queriedAttr.contains(attr)){
                queriedAttr.add(attr);
            }
            attr = term.getRhsField();
            if(!attr.equals("null") && !queriedAttr.contains(attr)){
                queriedAttr.add(attr);
            }
        }
    }

    public static void getqueriedAttrAll(QueryData data){
        
        //add projected attrs
        for (String fldName : data.projectFields()) {
            if(!queriedAttrAll.contains(fldName)){
                queriedAttrAll.add(fldName);
            }
        }
        //add aggregate attrs
        if (data.aggregationFn() != null)
			for (AggregationFn aggFn : data.aggregationFn()) {
				String aggFld = aggFn.argumentFieldName();
                if(!queriedAttrAll.contains(aggFld)){
                    queriedAttrAll.add(aggFld);
                }
			}
        //add group by attrs
        if (data.groupFields() != null)
        for (String groupByFld : data.groupFields()) {
            if(!queriedAttrAll.contains(groupByFld)){
                queriedAttrAll.add(groupByFld);
            }
        }
        //add predicate attrs
        for (Term term : data.pred().getTerms()){
            String attr = term.getLhsField();
            if(!attr.equals("null") && !queriedAttrAll.contains(attr)){
                queriedAttrAll.add(attr);
            }
            attr = term.getRhsField();
            if(!attr.equals("null") && !queriedAttrAll.contains(attr)){
                queriedAttrAll.add(attr);
            }
        }
    }

    public Schema reduceSchema(Schema sch, List<String> attrs){
        Map<String, Type> fields = sch.getSchema();
        Map<String, Type> newFields = new HashMap<>();
		for(Map.Entry<String, Type> entry : fields.entrySet()){
			if(attrs.contains(entry.getKey())){
				newFields.put(entry.getKey(), entry.getValue());
			}
		}
        SortedSet<String> myFieldSet = new TreeSet<String>(newFields.keySet());
        Schema newSch = new Schema(myFieldSet, newFields);
		return newSch;
	}

    public static void print(){
        for(int i=0;i<queriedAttrAll.size();i++){
            System.out.println(queriedAttrAll.get(i));
        }
    }
    
}
