package org.vanilladb.core.query.planner;

/*
 * This class implements knobs to control the join implementations
 */
public class JoinKnob {
    static public boolean productJoin = true, nestedloop = true, hashjoin = true, indexjoin = true;
    static public boolean fastLearning = false;
    static public boolean rawRun = false;
    static public int joinNumber = 0;
    static public int completeScanNumber = 0;//used to denote how many joins in rhs scan has complete in fast learning phase

    public static void init(){
        productJoin = true;
        nestedloop = true;
        hashjoin = true;
        indexjoin = true;
        fastLearning = false;
    }

    public static boolean ready(){
        if(joinNumber == completeScanNumber){
            return true;
        }
        return false;
    }

    public static void enableRawRun(){
        rawRun = true;
    }

    public static void enableFastLearning(){
        fastLearning = true;
    }

    public static void disableProductJoin(){
        productJoin = false;
    }

    public static void disableNestLoopJoin(){
        nestedloop = false;
    }

    public static void disableHashJoin(){
        hashjoin = false;
    }

    public static void disableIndexJoin(){
        indexjoin = false;
    }

    public static void forceHashJoin(){
        productJoin = false;
        indexjoin = false;
        nestedloop = false;
    }

    public static void forceMultiBufferJoin(){
        indexjoin = false;
        nestedloop = false;
        hashjoin = false;
    }

    public static void forceIndexJoin(){
        nestedloop = false;
        hashjoin = false;
        productJoin = false;
    }
}
