package leveretconey.cocoa;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import leveretconey.cocoa.multipleStandard.DFSDiscovererMultiStdRankingFxTopK;
import leveretconey.cocoa.ranking.LODRFClassSquareAttriCountAver;
import leveretconey.cocoa.ranking.LODRFClassSquareSumOverAttriCount;
import leveretconey.cocoa.multipleStandard.DFSDiscovererWithMultipleStandard;
import leveretconey.cocoa.multipleStandard.DFSDiscovererWithMultipleStandard.ValidatorType;
import leveretconey.cocoa.ranking.LODRankingFunction;
import leveretconey.cocoa.twoSideExpand.BruteForceALODValidatorUsingSP;
import leveretconey.cocoa.twoSideExpand.DFSDiscovererG1;
import leveretconey.cocoa.twoSideExpand.DFSDiscovererG3;
import leveretconey.cocoa.twoSideExpand.RandomSampleEstimatingALODValidator;
import leveretconey.dependencyDiscover.MinimalityChecker.ALODMinimalityCheckerUseFD;
import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.SortedPartition.ImprovedTwoSideSortedPartition;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.MinimalityChecker.ALODMinimalityChecker;
import leveretconey.dependencyDiscover.Data.DataFormatConverter;
import leveretconey.dependencyDiscover.Data.DataFormatConverter.DataFormatConverterConfig;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Discoverer.ALODDiscoverer;
import leveretconey.dependencyDiscover.MinimalityChecker.LODMinimalityChecker;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithAccurateBound;
import leveretconey.fastod.CanonicalOD;
import leveretconey.fastod.FasTOD;
import leveretconey.fastod.StrippedPartition;
import leveretconey.orderap.ORDERAP;
import leveretconey.util.Gateway;
import leveretconey.util.Statistics;
import leveretconey.util.TimeStatistics;
import leveretconey.util.TimedJob;
import leveretconey.util.Timer;
import leveretconey.util.Util;

@SpringBootTest
public class Tests {

    public static final String DATA_PATH ="data/debugData/DBT_250k_1.csv";
    public static final String DIRECTORY_PATH ="data/experimentData";
    public static final String DATA_OUTPUT_PATH="";
    public static final String DEPENDENCY_STRING="1<=->2<=";
    public static final boolean useData=true;
    public static final boolean useRawData=false;
    public static final boolean useDependency=false;
    public static final boolean forgeData=false;
    public static final int logLevel= Gateway.LogGateway.DEBUG;
    public static final boolean hasTimeLimit=false;
    private static final StringBuilder debugInfo=new StringBuilder();
    public static final String debugOutputPath="";

    long timeOutTime=10*60*1000;
    double errorRateThreshold=0.05;


    private LexicographicalOrderDependency dependency;
    private DataFrame data;

    @BeforeEach
    public void prepare() {
        Gateway.LogGateway.setLevel(logLevel);
        prepareDataSet();
        prepareDependency();
    }

    @AfterEach
    public void materialize(){
        if (debugInfo.length()>0 && !"".equals(debugOutputPath)){
            Util.toFile(debugInfo.toString(),debugOutputPath);
        }
    }

    void prepareDataSet(){
        if (useData){
            if (useRawData){
                prePareDataSetFromRawData();
            }else if (forgeData){
                data=DataFrame.randomDataFrame(100,5,1,2
                        ,(tuple,column,randomResult)->{
                            return column%2==0?tuple+randomResult:-tuple+randomResult;
                        });
            }else {
                data=DataFrame.fromCsv(DATA_PATH);
            }
            if (DATA_OUTPUT_PATH!=null && ! "".equals(DATA_OUTPUT_PATH)) {
                data.toCsv(DATA_PATH);
            }
            Util.out(String.format("数据集大小：%d行%d列",data.getTupleCount(),data.getColumnCount()));
        }
    }
    void prePareDataSetFromRawData(){
        File file=new File(DATA_PATH);
        DataFormatConverter converter=new DataFormatConverter();
        DataFormatConverterConfig config=new DataFormatConverterConfig(file.getAbsolutePath());

        config.hasHead=true;
        config.outputPath="data/temp/"+file.getName();
        config.preFilters=new DataFormatConverter.DataFramePreFilter[]
                {
                        DataFormatConverter.tooManyNullColumnPreFilter,
                        DataFormatConverter.tooManyNullTuplePreFilter,
                };
        config.postFilters=new DataFormatConverter.DataFramePostFilter[]
                {
                        DataFormatConverter.singleOrdinarityColumnPostFilter,
                        DataFormatConverter.getPostRandomFilter(50000,100000)
//                        DataFormatConverter.nearlyConstantColumnFilter
                };
        converter.convert(config);
        data=DataFrame.fromCsv(config.outputPath);

    }
    void prepareDependency(){
        if (useDependency) {
            dependency=LexicographicalOrderDependency.fromString(DEPENDENCY_STRING);
        }
    }

    void convertRawData(){
        File directory=new File("data/originaldata");
        String outputDirectory="data/experimentData";
        DataFormatConverter converter=new DataFormatConverter();
        Util.clearDirectories(new String[]{outputDirectory});
        for (File file : directory.listFiles()) {
            System.gc();
            try {
//                DataFormatConverter converter=new DataFormatConverter();
                DataFormatConverterConfig config=new DataFormatConverterConfig(file.getAbsolutePath());
                config.hasHead=true;
                config.outputPath=outputDirectory+"/"+file.getName();
//                config.preFilters=new DataFormatConverter.DataFramePreFilter[]
//                        {
////                                DataFormatConverter.tooManyNullColumnPreFilter,
////                                DataFormatConverter.tooManyNullTuplePreFilter,
//                        };
//                config.postFilters=new DataFormatConverter.DataFramePostFilter[]
//                        {
//                                //DataFormatConverter.singleOrdinarityColumnPostFilter,
////                                DataFormatConverter.nearlyConstantColumnFilter
//                        };
                converter.convert(config);
                Util.out("converting succeeds: "+file.getPath());
            }catch (Exception e){
                Util.out("converting fails: "+file.getPath());
                e.printStackTrace();
            }
        }
    }

    void renameDataFiles(){
        File directory=new File("data/experimentData");
        for(File file:directory.listFiles()){
            DataFrame data=DataFrame.fromCsv(file.getAbsolutePath());
            String fileName=file.getName();
            int lastDotPosition=fileName.lastIndexOf('.');
            if(lastDotPosition!=-1)
                fileName=fileName.substring(0,lastDotPosition);
            String newName=String.format("%s/%s %d %d.csv",directory.getAbsolutePath(),
                    fileName,data.getTupleCount(),data.getColumnCount());
            file.renameTo(new File(newName));
            Util.out("rename succeed: "+file.getPath());
        }
    }

    void testSPIntersect(){
        SortedPartition originSp=new SortedPartition(data);
        SortedPartition improvedSp=new SortedPartition(data);
        SingleAttributePredicateList list=SingleAttributePredicateList.fromString("1<=,2<=,3>=,4>=");
        for (SingleAttributePredicate predicate : list) {
            SortedPartition another=new SortedPartition(data,new SingleAttributePredicateList(predicate));
            TimeStatistics.TimeStopper timer = TimeStatistics.start("原来的sp扩展算法");
            originSp.intersectOld(another);
            timer.stop();
            timer = TimeStatistics.start("改进的sp扩展算法");
            improvedSp.intersect(another);
            timer.stop();
            Util.out(originSp.equals(improvedSp)?"结果正确":"结果错误");
        }
        TimeStatistics.printStatistics();
    }


    /**
     * converting succeeds: data\originaldata\adult.csv
     * pre filter: delete 5 columns and 0 tuples
     * post filter: delete 4 columns and 0 tuples
     * converting succeeds: data\originaldata\ATOM.csv
     * pre filter: delete 15 columns and 0 tuples
     * post filter: delete 8 columns and 0 tuples
     * converting succeeds: data\originaldata\ATOM_SITES.csv
     * post filter: delete 4 columns and 0 tuples
     * converting succeeds: data\originaldata\CENSUS.csv
     * pre filter: delete 1 columns and 0 tuples
     * converting succeeds: data\originaldata\CLASSIFICATION.csv
     * post filter: delete 1 columns and 0 tuples
     * converting succeeds: data\originaldata\DITAG_FEATURE.csv
     * pre filter: delete 5 columns and 0 tuples
     * post filter: delete 22 columns and 0 tuples
     * converting succeeds: data\originaldata\ENTYTYSRCGEN.csv
     * converting succeeds: data\originaldata\fd-reduced-30.csv
     * converting succeeds: data\originaldata\fd_reduced_15.csv
     * pre filter: delete 13 columns and 0 tuples
     * post filter: delete 1 columns and 0 tuples
     * converting succeeds: data\originaldata\foursquare_spots.csv
     * pre filter: delete 1 columns and 0 tuples
     * post filter: delete 2 columns and 0 tuples
     * converting succeeds: data\originaldata\IMAGE.csv
     * pre filter: delete 21 columns and 0 tuples
     * post filter: delete 18 columns and 0 tuples
     * converting succeeds: data\originaldata\ncvoter_Statewide.10000r.csv
     * pre filter: delete 1 columns and 0 tuples
     * post filter: delete 7 columns and 0 tuples
     * converting succeeds: data\originaldata\PDBX_DATABASE_STATUS.csv
     * pre filter: delete 26 columns and 0 tuples
     * converting succeeds: data\originaldata\REFLNS.csv
     * pre filter: delete 1 columns and 0 tuples
     * post filter: delete 2 columns and 0 tuples
     * converting succeeds: data\originaldata\SG_BIOENTRY.csv
     * post filter: delete 3 columns and 0 tuples
     * converting succeeds: data\originaldata\STRUCT_SHEET_RANGE.csv
     * pre filter: delete 1 columns and 0 tuples
     * converting succeeds: data\originaldata\ uce-results-by-school-2011-2015.csv
     * rename succeed: data\experimentData\adult.csv
     * rename succeed: data\experimentData\ATOM.csv
     * rename succeed: data\experimentData\ATOM_SITES.csv
     * rename succeed: data\experimentData\CENSUS.csv
     * rename succeed: data\experimentData\CLASSIFICATION.csv
     * rename succeed: data\experimentData\DITAG_FEATURE.csv
     * rename succeed: data\experimentData\ENTYTYSRCGEN.csv
     * rename succeed: data\experimentData\fd-reduced-30.csv
     * rename succeed: data\experimentData\fd_reduced_15.csv
     * rename succeed: data\experimentData\foursquare_spots.csv
     * rename succeed: data\experimentData\IMAGE.csv
     * rename succeed: data\experimentData\ncvoter_Statewide.10000r.csv
     * rename succeed: data\experimentData\PDBX_DATABASE_STATUS.csv
     * rename succeed: data\experimentData\REFLNS.csv
     * rename succeed: data\experimentData\SG_BIOENTRY.csv
     * rename succeed: data\experimentData\STRUCT_SHEET_RANGE.csv
     * rename succeed: data\experimentData\ uce-results-by-school-2011-2015.csv
     */
    void convertAndRename(){
        convertRawData();
        renameDataFiles();
    }

    void testValidators(){
        ALODValidator[] validators=new ALODValidator[]{
                new BruteForceALODValidatorUsingSP(),
                new RandomSampleEstimatingALODValidator(10000,new Random(1)),
        };
        for (int c1 = 1; c1 <= data.getColumnCount(); c1++) {
            for (int c2 = 1; c2 <= data.getColumnCount(); c2++) {
                if (c1==c2){
                    continue;
                }
                LexicographicalOrderDependency dependency=LexicographicalOrderDependency.fromString(String.format("%d<=->%d<=",c1,c2));
                for (ALODValidator validator : validators) {
                    Util.out(String.format("%s %s 验证 %s",validator.validate(data,dependency),validator.getClass().getSimpleName(),dependency));
                }
                Util.out("");

                dependency=LexicographicalOrderDependency.fromString(String.format("%d>=->%d<=",c1,c2));
                for (ALODValidator validator : validators) {
                    Util.out(String.format("%s %s 验证 %s",validator.validate(data,dependency),validator.getClass().getSimpleName(),dependency));
                }
                Util.out("");
            }
        }
    }

    /**
     *     0.5%    1%      5%      10%    2%
     * g1  1296    1296    12194   33674  2200
     * g3  134     873     31599   33268  120000+
     * g13 134     873     31573   33268
     * g13 0.01+0.05: 24213
     */

    void testFlight(){
        DataFrame data;
        ALODDiscoverer discoverer;
        Collection<LexicographicalOrderDependency> result;

        data=DataFrame.fromCsv("data/debugData/flight_200k.csv");
        discoverer=new DFSDiscovererWithMultipleStandard(ValidatorType.G3,0.03);
        result=discoverer.discover(data,0.03);
        printStatistics();
        resetStatistics();

        data=DataFrame.fromCsv("data/debugData/flight_250k_12.csv");
        discoverer=new DFSDiscovererWithMultipleStandard(ValidatorType.G3,0.03);
        result=discoverer.discover(data,0.03);
        printStatistics();
        resetStatistics();

    }

    void trySingleData(){
        ValidatorType[][] validators=new ValidatorType[][]{
                {ValidatorType.G3},
        };
        double[][] errorRates=new double[][]{
                {0.03},
        } ;
        for (int i = 0; i < validators.length; i++) {
            ALODDiscoverer discoverer=new DFSDiscovererWithMultipleStandard(validators[i],errorRates[i]);
            Collection<LexicographicalOrderDependency> ods = discoverer.discover(data, 0.03);
            for (LexicographicalOrderDependency od : ods) {
                Util.out(od);
            }
        }

    }

    void testOrder(){
        ALODDiscoverer discoverer=new DFSDiscovererWithMultipleStandard(ValidatorType.G1,0.02);
        Collection<LexicographicalOrderDependency> result;
//        result = discoverer.discover(data, 0.02);
//        printStatistics();
//        resetStatistics();
//        for (LexicographicalOrderDependency od : result) {
//            Util.out(od);
//        }

        discoverer=new ORDERAP();
        result = discoverer.discover(data, 0.02);
        for (LexicographicalOrderDependency od : result) {
            Util.out(od);
        }


    }

    void compareG1G3(){
        Collection<LexicographicalOrderDependency> resultg1 = new DFSDiscovererG1().discover(data, errorRateThreshold);
        Collection<LexicographicalOrderDependency> resultg3 = new DFSDiscovererG3().discover(data, errorRateThreshold);

        Set<LexicographicalOrderDependency> dependencies=new HashSet<>(resultg1);
        resultg3.removeIf(dependency1 -> dependencies.contains(dependency1));
        printFirstNODS(resultg3,10000);

    }

    void testALODPath(){
        String odString="4<=,6<=->3<=,5<=";
        LexicographicalOrderDependency odPrototype=LexicographicalOrderDependency.fromString(odString);
        LexicographicalOrderDependency od=new LexicographicalOrderDependency();
        LODRankingFunction rankingFunction=new LODRFClassSquareSumOverAttriCount();

        od.right.add(odPrototype.right.get(0));
        od.left.add(odPrototype.left.get(0));
        ImprovedTwoSideSortedPartition parent=new ImprovedTwoSideSortedPartition(data,od);

        while (true){
            ImprovedTwoSideSortedPartition isp=new ImprovedTwoSideSortedPartition(data,od);
            ValidationResultWithAccurateBound er = isp.validateForALODWithG1();
            Util.out(String.format("%s (%s): %.4f",od,er,rankingFunction.getRanking(od,isp)));
            if (od.length() >= odPrototype.length()) {
                break;
            }
            if (er.isValid(errorRateThreshold)) {
                parent=parent.deepCloneAndIntersect(data,odPrototype.right.get(od.right.size()),false);
                od.right.add(odPrototype.right.get(od.right.size()));
            } else {
                parent=parent.deepCloneAndIntersect(data,odPrototype.left.get(od.left.size()),true);
                od.left.add(odPrototype.left.get(od.left.size()));
            }
        }
    }

    void testSomeODs(){
        LexicographicalOrderDependency od1=LexicographicalOrderDependency.fromString("5<=->3<=");
        LexicographicalOrderDependency od2=LexicographicalOrderDependency.fromString("5<=->3<=,6<=");
//        LexicographicalOrderDependency od2=LexicographicalOrderDependency.fromString("1<=->2<=");

//        ImprovedTwoSideSortedPartition isp=new ImprovedTwoSideSortedPartition(data,od2);
//        Util.out(String.format("g1:%s, g3:%s",isp.validateForALODWithG1(),isp.validateForALODWithG3()));


        ImprovedTwoSideSortedPartition isp=new ImprovedTwoSideSortedPartition(data,od1);
        ImprovedTwoSideSortedPartition isp2=new ImprovedTwoSideSortedPartition(data,od2);
        Util.out(String.format("g1:%s, g3:%s",isp.validateForALODWithG1(),isp.validateForALODWithG3()));
        Util.out(String.format("g1:%s, g3:%s",isp2.validateForALODWithG1(),isp2.validateForALODWithG3()));
    }

    void tryDatasetsInOneDirectory(){
        File directory=new File(DIRECTORY_PATH);

        for (File file : directory.listFiles()) {
            DataFrame data=DataFrame.fromCsv(file.getPath());
            ALODDiscoverer discoverer=new DFSDiscovererWithMultipleStandard();
            discover(discoverer,data,file.getName());
        }
    }

    void testALODMinimalityChecker(){
        LODMinimalityChecker checker=new ALODMinimalityChecker();
        checker.insert(LexicographicalOrderDependency.fromString("1<=,2<=->3<=,4<="));
        checker.insert(LexicographicalOrderDependency.fromString("2<=,3<=->4>="));
        checker.insert(LexicographicalOrderDependency.fromString("1<=->4>="));
        checker.insert(LexicographicalOrderDependency.fromString("4<=,5<=,6<=->7<="));

        Assert.assertFalse(checker.isMinimal(
                SingleAttributePredicateList.fromString("2<=,3<=")
                ,SingleAttributePredicate.fromString("4>=")));

        Assert.assertTrue(checker.isMinimal(
                SingleAttributePredicateList.fromString("2<=,3>=")
                ,SingleAttributePredicate.fromString("4>=")));

        Assert.assertTrue(checker.isMinimal(
                SingleAttributePredicateList.fromString("2<=")
                ,SingleAttributePredicate.fromString("4>=")));

        Assert.assertFalse(checker.isMinimal(
                SingleAttributePredicateList.fromString("1<=,2>=,3>=")
                ,SingleAttributePredicate.fromString("4<=")));

        Assert.assertFalse(checker.isMinimal(
                SingleAttributePredicateList.fromString("1<=,2<=")
                ,SingleAttributePredicate.fromString("3<=")));

        Assert.assertTrue(checker.isMinimal(
                SingleAttributePredicateList.fromString("1<=,4>=,5<=,6>=")
                ,SingleAttributePredicate.fromString("7>=")));

        Assert.assertFalse(checker.isMinimal(
                SingleAttributePredicateList.fromString("1<=,4>=,5>=,6>=")
                ,SingleAttributePredicate.fromString("7>=")));

        Assert.assertTrue(checker.isMinimal(
                SingleAttributePredicateList.fromString("1<=,4<=")
                ,SingleAttributePredicate.fromString("7<="))
        );

        Assert.assertFalse(checker.isMinimal(
                SingleAttributePredicateList.fromString("1<=")
                ,SingleAttributePredicate.fromString("4>="))
        );
        Assert.assertFalse(checker.isMinimal(
                SingleAttributePredicateList.fromString("1>=")
                ,SingleAttributePredicate.fromString("4<="))
        );
        Assert.assertFalse(checker.isMinimal(
                SingleAttributePredicateList.fromString("1<=,3<=")
                ,SingleAttributePredicate.fromString("4>="))
        );
        Assert.assertFalse(checker.isMinimal(
                SingleAttributePredicateList.fromString("1<=,2<=")
                ,SingleAttributePredicate.fromString("3<="))
        );
        Assert.assertTrue(checker.isMinimal(
                SingleAttributePredicateList.fromString("1<=")
                ,SingleAttributePredicate.fromString("4<="))
        );
        Assert.assertTrue(checker.isMinimal(
                SingleAttributePredicateList.fromString("2<=,3<=")
                ,SingleAttributePredicate.fromString("4<="))
        );
    }

    @SuppressWarnings("all")
    void discover(ALODDiscoverer discoverer,DataFrame data,String dataSetName){
        Util.out(String.format("%s开始尝试数据集 %s,",
                discoverer.getClass().getSimpleName(),dataSetName));
        resetStatistics();
        Timer timer=new Timer();
        Collection<LexicographicalOrderDependency> result;
        if (hasTimeLimit) {
            result = new TimedJob<Collection<LexicographicalOrderDependency>>()
                    .start(timeOutTime, () -> discoverer.discover(data, errorRateThreshold));
            if (result == null) {
                Util.out(String.format("数据集 %s 中运行超时", dataSetName));
            } else {
                Util.out(String.format("数据集 %s 中发现%d个od，用时%.3f秒",
                        dataSetName, result.size(), timer.getTimeUsed() / 1000.0));
            }
        }else {
            result=discoverer.discover(data,errorRateThreshold);
            Util.out(String.format("数据集 %s 中发现%d个od，用时%.3f秒", dataSetName, result.size(), timer.getTimeUsed() / 1000.0));
        }
        printStatistics();
        for (LexicographicalOrderDependency orderDependency : result) {
            debugInfo.append(orderDependency).append(' ').append(new ImprovedTwoSideSortedPartition(data,orderDependency).validateForALODWithG1()).append("\n");
        }
        debugInfo.append("\n\n\n\n\n\n");
        Util.out("");
    }
    void checkALODs(Collection<LexicographicalOrderDependency> dependencies,double errorRateThreshold){
        ALODValidator validator=new BruteForceALODValidatorUsingSP();
        int errorCount=0;
        for (LexicographicalOrderDependency dependency : dependencies) {
            if(!validator.validate(data,dependency).isValid(errorRateThreshold)){
                errorCount++;
            }
        }
        Util.out(String.format("检查了%d个od，有%d个是不成立的",dependencies.size(),errorCount));
    }
    void printFirstNODS(Collection<LexicographicalOrderDependency> ods,int n){
        for (LexicographicalOrderDependency od : ods) {
            if (n--==0){
                break;
            }
            Util.out(od);
        }
    }
    void resetStatistics(){
        TimeStatistics.reset();
        Statistics.reset();
    }
    void printStatistics(){
        TimeStatistics.printStatistics();
        Statistics.printStstistics();
    }

    void testRankingTopk(){
        ALODDiscoverer discoverer=new DFSDiscovererMultiStdRankingFxTopK
                (ValidatorType.G1,0.02,20,new LODRFClassSquareAttriCountAver(data,0.18),4);
        Collection<LexicographicalOrderDependency> ods = discoverer.discover(data, errorRateThreshold);
        for (LexicographicalOrderDependency od : ods) {
            Util.out(od);
        }
    }

    void testRankingFunction(){
        DFSDiscovererWithMultipleStandard discoverer = new DFSDiscovererWithMultipleStandard(ValidatorType.G1, 0.02);
        Collection<LexicographicalOrderDependency> ods = discoverer.discover(data, 0.02);
        LODRankingFunction[] rankingFunctions=new LODRankingFunction[11];
        for (int i = 0; i < 11; i++) {
            rankingFunctions[i]=new LODRFClassSquareAttriCountAver(data,0.1*i);
        }
        for (LexicographicalOrderDependency od : ods) {
            ImprovedTwoSideSortedPartition isp=new ImprovedTwoSideSortedPartition(data,od);
            StringBuilder sb=new StringBuilder();
            sb.append(od);
            for (LODRankingFunction rankingFunction : rankingFunctions) {
                sb.append(String.format(";%.4f",rankingFunction.getRanking(od,isp)));
            }
            Util.out(sb.toString());
        }
    }

    void testFastOD(){
        FasTOD fasTOD=new FasTOD(10*60*60*1000,0.01);
        List<CanonicalOD> ods = fasTOD.discover(data);
        for (CanonicalOD od : ods) {
            Util.out(od);
        }
    }

    void testFastODER(){
        for (int i = 0; i < data.getColumnCount(); i++) {
            for (int j = i+1; j < data.getColumnCount(); j++) {
                LexicographicalOrderDependency od=LexicographicalOrderDependency
                        .fromString(String.format("%d<=->%d<=",i+1,j+1));
                Util.out(od);
                Util.out(new ImprovedTwoSideSortedPartition(data,od).validateForALODWithG3());

                StrippedPartition sp=new StrippedPartition(data);
                double er=sp.swapRemoveCount(SingleAttributePredicate.getInstance
                        (i, Operator.lessEqual),j)/(double)data.getTupleCount();
                Util.out(String.format("%.3f%%",er*100));
                Util.out("");
            }
        }

    }

    void testFDMinimalChecker(){
        ALODMinimalityCheckerUseFD checker=new ALODMinimalityCheckerUseFD();
        checker.insert(LexicographicalOrderDependency.fromString("1<=,2<=->3<="));
        checker.insert(LexicographicalOrderDependency.fromString("2<=,3<=->4>="));
        checker.insert(LexicographicalOrderDependency.fromString("5<=->6>="));

        Assert.assertFalse(checker.isMinimal
                (SingleAttributePredicateList.fromString("1<=,2<=,7<=,3<=")));

        Assert.assertFalse(checker.isMinimal
                (SingleAttributePredicateList.fromString("1<=,2<=,3<=")));

        Assert.assertFalse(checker.isMinimal
                (SingleAttributePredicateList.fromString("1<=,2>=,3>=")));

        Assert.assertTrue(checker.isMinimal
                (SingleAttributePredicateList.fromString("1<=,3<=")));

        Assert.assertTrue(checker.isMinimal
                (SingleAttributePredicateList.fromString("1<=,3>=")));

    }

    void testRecall(){
        DataFrame[] datas={
                DataFrame.fromCsv("data/debugData/flight_10k_0.01noise_1.csv"),
        };
        LexicographicalOrderDependency[] ods={
                LexicographicalOrderDependency.fromString("1<=->2<="),
                LexicographicalOrderDependency.fromString("3<=->4<="),
                LexicographicalOrderDependency.fromString("5<=->6<="),
                LexicographicalOrderDependency.fromString("10<=->7<=,8<=,9<="),
                LexicographicalOrderDependency.fromString("11<=->12<="),
        };

        Util.out("g1:");
        for (LexicographicalOrderDependency od : ods) {
            for (DataFrame data : datas) {
                ImprovedTwoSideSortedPartition isp=new ImprovedTwoSideSortedPartition(data,od);
                Util.out(String.format("%s : %s",od,isp.validateForALODWithG1()));
            }
            Util.out("");
        }

        Util.out("\n\ng3:");
        for (LexicographicalOrderDependency od : ods) {
            for (DataFrame data : datas) {
                ImprovedTwoSideSortedPartition isp=new ImprovedTwoSideSortedPartition(data,od);
                Util.out(String.format("%s : %s",od,isp.validateForALODWithG3()));
            }
            Util.out("");
        }
    }
}
