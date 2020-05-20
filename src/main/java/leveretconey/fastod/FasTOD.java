package leveretconey.fastod;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.util.Timer;
import leveretconey.util.Util;

public class FasTOD {


    private long computeODTime=0;
    private long pruneLevelTime=0;
    private long calculateNextLevelTime=0;

    private boolean complete=true;
    private Timer beginTimer;

    private long timeLimit=5*60*60*1000;
    //M
    private Set<CanonicalOD> result;
    //L
    private List<Set<AttributeSet>> contextInEachLevel;
    //cc
    private HashMap<AttributeSet, AttributeSet> cc;
    //cs
    private HashMap<AttributeSet, Set<AttributePair>> cs;
    //l
    private int level;
    //R
    private AttributeSet schema;

    private DataFrame data;

    private boolean output;

    private double errorRateThreshold=-1f;

    public FasTOD(long timeLimit, boolean output, double errorRateThreshold) {
        this.timeLimit = timeLimit;
        this.output = output;
        this.errorRateThreshold = errorRateThreshold;
    }

    public FasTOD(long timeLimit, boolean output) {
        this.timeLimit = timeLimit;
        this.output = output;
    }

    void out(Object s){
        if (output){
            Util.out(s);
        }
    }


    private boolean timeUp(){
        return beginTimer.getTimeUsed()>=timeLimit;
    }

    private void ccPut(AttributeSet key, AttributeSet attributeSet){
        if(!cc.containsKey(key))
            cc.put(key,new AttributeSet());
        cc.put(key,attributeSet);
    }

    private void ccUnion(AttributeSet key, AttributeSet attributeSet){
        if(!cc.containsKey(key))
            cc.put(key,new AttributeSet());
        cc.put(key,cc.get(key).union(attributeSet));
    }

    private void ccPut(AttributeSet key, int attribute){
        if(!cc.containsKey(key))
            cc.put(key,new AttributeSet());
        cc.put(key,cc.get(key).addAttribute(attribute));
    }
    private AttributeSet ccGet(AttributeSet key){
        if(!cc.containsKey(key))
            cc.put(key,new AttributeSet());
        return cc.get(key);
    }
    private void csPut(AttributeSet key, AttributePair value){
        if(!cs.containsKey(key))
            cs.put(key,new HashSet<>());
        cs.get(key).add(value);
    }

    private Set<AttributePair> csGet(AttributeSet key){
        if(!cs.containsKey(key))
            cs.put(key,new HashSet<>());
        return cs.get(key);
    }

    public boolean isComplete() {
        return complete;
    }

    private void initialize(DataFrame data){
        beginTimer=new Timer();
        this.data=data;
        result=new TreeSet<>();
        cc=new HashMap<>();
        cs=new HashMap<>();
        contextInEachLevel=new ArrayList<>();
        contextInEachLevel.add(new HashSet<>());
        AttributeSet emptySet=new AttributeSet();
        //对于这行代码我有疑问,我认为是这样的
        contextInEachLevel.get(0).add(emptySet);

        schema=new AttributeSet();
        for (int i = 0; i < data.getColumnCount(); i++) {
            schema=schema.addAttribute(i);
            ccPut(emptySet,i);
        }

        level=1;

        HashSet<AttributeSet> level1Candidates=new HashSet<>();
        for (int i = 0; i < data.getColumnCount(); i++) {
            AttributeSet singleAttribute=emptySet.addAttribute(i);
            level1Candidates.add(singleAttribute);
        }

        contextInEachLevel.add(level1Candidates);
    }

    public Set<CanonicalOD> discover(DataFrame data){

        Timer timer=new Timer();
        initialize(data);
        while (contextInEachLevel.get(level).size()!=0){
            out(String.format("--------------------------\n第%d层开始\n--------------------------",level));
            computeODs();
            if (timeUp()){
                break;
            }
            pruneLevels();
            calculateNextLevel();
            if (timeUp()){
                break;
            }
            level++;
        }
        int fdCount=0,ocdCount=0;
        for (int i = 0; i < 10; i++) {
            out("");
        }
        for (CanonicalOD od : result) {
            out(od);
            if(od.left==-1)
                fdCount++;
            else
                ocdCount++;
        }
        out("最终统计");
        if(isComplete()){
            out("正常终止");
        }else {
            out("到达时间上限终止");
        }
        out("共发现"+result.size()+"个od");
        out("其中有"+fdCount+"个fd");
        out("其中有"+ocdCount+"个ocd");
        out("split检查次数"+ CanonicalOD.splitCheckCount);
        out("swap检查次数"+ CanonicalOD.splitCheckCount);
        out("product用时"+ StrippedPartition.mergeTime/1000.0+"s");
        out("check用时"+ StrippedPartition.validateTime/1000.0+"s");
        out("clone用时"+ StrippedPartition.cloneTime/1000.0+"s");
        out("computeOD用时"+computeODTime/1000.0+"s");
        out("pruneLevel用时"+pruneLevelTime/1000.0+"s");
        out("calculateNextLevel用时"+calculateNextLevelTime/1000.0+"s");
        out("共用时"+timer.getTimeUsed()/1000.0+"s");
        return result;
    }
    private void computeODs(){
        Timer timer=new Timer();
        Set<AttributeSet> contextThisLevel=contextInEachLevel.get(level);
        for(AttributeSet context : contextThisLevel){
            if(timeUp()){
                complete=false;
                return;
            }
            AttributeSet contextCC=schema;
            for(int attribute : context){
                contextCC=contextCC.intersect(ccGet(context.deleteAttribute(attribute)));
            }
            ccPut(context,contextCC);
            if(level==2){
                 for (int i = 0; i < data.getColumnCount(); i++) {
                     for (int j = 0; j < data.getColumnCount(); j++) {
                         if(i==j)
                             continue;
                         csPut(new AttributeSet(Arrays.asList(i,j)),new AttributePair(i,j));
                     }
                 }
            }else if(level>2){
                Set<AttributePair> candidateCsPairSet=new HashSet<>();
                for(int attribute : context){
                    candidateCsPairSet.addAll(csGet(context.deleteAttribute(attribute)));
                }
                for(AttributePair attributePair : candidateCsPairSet ){
                    AttributeSet contextDeleteAB=context
                            .deleteAttribute(attributePair.attribute1)
                            .deleteAttribute(attributePair.attribute2);
                    boolean addContext=true;
                    for(int attribute : contextDeleteAB){
                        if(!csGet(context.deleteAttribute(attribute)).contains(attributePair)){
                            addContext=false;
                            break;
                        }
                    }
                    if (addContext){
                        csPut(context,attributePair);
                    }
                }
            }
        }

        for(AttributeSet context:contextThisLevel){
            if(timeUp()){
                complete=false;
                return;
            }
            AttributeSet contextIntersectCCContext=context.intersect(ccGet(context));
            for(int attribute :contextIntersectCCContext){
                CanonicalOD od=
                        new CanonicalOD(context.deleteAttribute(attribute),attribute);
                if(od.isValid(data,errorRateThreshold)){
                    result.add(od);
                    out("发现od: "+od);
                    ccPut(context,ccGet(context).deleteAttribute(attribute));
                    for(int i :  schema.difference(context)){
                        ccPut(context,ccGet(context).deleteAttribute(i));
                    }
                }
            }
            List<AttributePair> attributePairsToRemove=new ArrayList<>();
            for (AttributePair attributePair:csGet(context)){
                int a=attributePair.attribute1;
                int b=attributePair.attribute2;
                if(!ccGet(context.deleteAttribute(b)).containAttribute(a)
                || !ccGet(context.deleteAttribute(a)).containAttribute(b)){
                    attributePairsToRemove.add(attributePair);
                }else {
                    CanonicalOD od =
                            new CanonicalOD(context.deleteAttribute(a).deleteAttribute(b), a, b);
                    if (od.isValid(data,errorRateThreshold)) {
                        out("发现od: " + od);
                        result.add(od);
                        attributePairsToRemove.add(attributePair);

                    }
                }

            }
            for (AttributePair attributePair : attributePairsToRemove) {
                csGet(context).remove(attributePair);
            }
        }
        computeODTime+=timer.getTimeUsed();
    }
    private void pruneLevels(){
        Timer timer=new Timer();
        if (level>=2){
            List<AttributeSet> nodesToRemove=new ArrayList<>();
            for (AttributeSet attributeSet : contextInEachLevel.get(level)) {
                if(ccGet(attributeSet).isEmpty()
                       && csGet(attributeSet).isEmpty()){
                    nodesToRemove.add(attributeSet);
                }
            }
            Set<AttributeSet> contexts=contextInEachLevel.get(level);
            for (AttributeSet attributeSet : nodesToRemove) {
                out("节点优化: "+attributeSet );
                contexts.remove(attributeSet);
            }
        }
        pruneLevelTime+=timer.getTimeUsed();
    }
    private void calculateNextLevel(){
        Timer timer=new Timer();
        Map<AttributeSet,List<Integer>> prefixBlocks=new HashMap<>();
        Set<AttributeSet> contextNextLevel=new HashSet<>();
        Set<AttributeSet> contextThisLevel=contextInEachLevel.get(level);

        for(AttributeSet attributeSet:contextThisLevel){
            for (Integer attribute : attributeSet) {
                AttributeSet prefix=attributeSet.deleteAttribute(attribute);
                if(!prefixBlocks.containsKey(prefix)){
                    prefixBlocks.put(prefix,new ArrayList<>());
                }
                prefixBlocks.get(prefix).add(attribute);
            }
        }

        for (Map.Entry<AttributeSet, List<Integer>> attributeSetListEntry : prefixBlocks.entrySet()) {
            if(timeUp()){
                complete=false;
                return;
            }
            AttributeSet prefix=attributeSetListEntry.getKey();
            List<Integer> singleAttributes=attributeSetListEntry.getValue();
            if(singleAttributes.size()<=1)
                continue;
            for (int i = 0; i < singleAttributes.size(); i++) {
                for (int j = i+1; j < singleAttributes.size(); j++) {
                    boolean createContext=true;
                    AttributeSet candidate=prefix.addAttribute(singleAttributes.get(i))
                            .addAttribute(singleAttributes.get(j));
                    for (Integer attribute : candidate) {
                        if(!contextThisLevel.contains(candidate.deleteAttribute(attribute))){
                            createContext=false;
                            break;
                        }
                    }
                    if(createContext){
                        contextNextLevel.add(candidate);
                    }
                }
            }
        }
        contextInEachLevel.add(contextNextLevel);
        calculateNextLevelTime+=timer.getTimeUsed();
    }

}
