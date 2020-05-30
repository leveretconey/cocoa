package leveretconey.fastod;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.util.Gateway;
import leveretconey.util.Timer;
import leveretconey.util.Util;


public class FasTOD {


    private final long timeLimit;
    private boolean complete=true;

    //M
    private List<CanonicalOD> result;
    //L
    private List<Set<AttributeSet>> contextInEachLevel;
    //cc
    private HashMap<AttributeSet,AttributeSet> cc;
    //cs
    private HashMap<AttributeSet, Set<AttributePair>> cs;
    //l
    private int level;
    //R
    private AttributeSet schema;

    private DataFrame data;

    private double errorRateThreshold=-1f;


    Gateway traversalGateway;

    //statistics
    int odcount=0,fdcount=0,ocdcount=0;

    Timer timer;
    void printStatistics(){
        if (traversalGateway.isOpen()){
            Util.out(String.format("当前时间%.3f秒，发现od%d个，其中fd%d个，ocd%d个,最后一个od是%s"
            ,timer.getTimeUsedInSecond(),fdcount+ocdcount,fdcount,ocdcount,result.size()>0?result.get(result.size()-1):""));
        }
    }

    public FasTOD(long timeLimit, double errorRateThreshold) {
        this.timeLimit = timeLimit;
        this.errorRateThreshold = errorRateThreshold;
    }

    public FasTOD(long timeLimit) {
        this.timeLimit = timeLimit;
    }





    private boolean timeUp(){
        return timer.getTimeUsed()>=timeLimit;
    }

    private void ccPut(AttributeSet key, AttributeSet attributeSet){
        if(!cc.containsKey(key))
            cc.put(key,new AttributeSet());
        cc.put(key,attributeSet);
    }

    private void ccUnion(AttributeSet key,AttributeSet attributeSet){
        if(!cc.containsKey(key))
            cc.put(key,new AttributeSet());
        cc.put(key,cc.get(key).union(attributeSet));
    }

    private void ccPut(AttributeSet key,int attribute){
        if(!cc.containsKey(key))
            cc.put(key,new AttributeSet());
        cc.put(key,cc.get(key).addAttribute(attribute));
    }
    private AttributeSet ccGet(AttributeSet key){
        if(!cc.containsKey(key))
            cc.put(key,new AttributeSet());
        return cc.get(key);
    }
    private void csPut(AttributeSet key,AttributePair value){
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
        traversalGateway = new Gateway.ComplexTimeGateway();
        timer = new Timer();
        this.data=data;
        result=new ArrayList<>();
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

    public List<CanonicalOD> discover(DataFrame data){

        initialize(data);
        while (contextInEachLevel.get(level).size()!=0){
            Util.out(String.format("第%d层开始",level));
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
        if (isComplete()){
            Util.out("FastOD算法正常结束");
        }else {
            Util.out("FastOD算法结束,运行超时");
        }
        Util.out(String.format("当前时间%.3f秒，发现od%d个，其中fd%d个，ocd%d个"
                ,timer.getTimeUsedInSecond(),fdcount+ocdcount,fdcount,ocdcount));
        return result;
    }
    private void computeODs(){
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
                         AttributeSet c=new AttributeSet(Arrays.asList(i,j));
                         csPut(c, new AttributePair(SingleAttributePredicate.getInstance
                                 (i, Operator.greaterEqual),j));
                         csPut(c, new AttributePair(SingleAttributePredicate.getInstance
                                 (i, Operator.lessEqual),j));
                     }
                 }
            }else if(level>2){
                Set<AttributePair> candidateCsPairSet=new HashSet<>();
                for(int attribute : context){
                    candidateCsPairSet.addAll(csGet(context.deleteAttribute(attribute)));
                }
                for(AttributePair attributePair : candidateCsPairSet ){
                    AttributeSet contextDeleteAB=context
                            .deleteAttribute(attributePair.left.attribute)
                            .deleteAttribute(attributePair.right);
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
                    fdcount++;
                    ccPut(context,ccGet(context).deleteAttribute(attribute));
                    for(int i :  schema.difference(context)){
                        ccPut(context,ccGet(context).deleteAttribute(i));
                    }
                    printStatistics();
                }
            }
            List<AttributePair> attributePairsToRemove=new ArrayList<>();
            for (AttributePair attributePair:csGet(context)){
                int a=attributePair.left.attribute;
                int b=attributePair.right;
                if(!ccGet(context.deleteAttribute(b)).containAttribute(a)
                || !ccGet(context.deleteAttribute(a)).containAttribute(b)){
                    attributePairsToRemove.add(attributePair);
                }else {
                    CanonicalOD od =
                            new CanonicalOD(context.deleteAttribute(a).deleteAttribute(b),
                                    attributePair.left, b);
                    if (od.isValid(data,errorRateThreshold)) {
                        ocdcount++;
                        result.add(od);
                        attributePairsToRemove.add(attributePair);
                    }
                    printStatistics();
                }

            }
            for (AttributePair attributePair : attributePairsToRemove) {
                csGet(context).remove(attributePair);
            }
        }
    }
    private void pruneLevels(){
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
                contexts.remove(attributeSet);
            }
        }
    }
    private void calculateNextLevel(){
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
                    for (int attribute : candidate) {
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
    }

}
