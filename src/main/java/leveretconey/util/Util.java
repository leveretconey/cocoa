package leveretconey.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import javafx.print.Collation;

public class Util {
    private Util(){}

    public static String toAString(Object o){
        StringBuilder sb=new StringBuilder();
        objectToString(o,sb);
        return sb.toString();
    }

    private static void objectToString(Object o,StringBuilder sb){
        if(o==null){
            sb.append("null");
        }else if(o.getClass().isArray()){
            arrayToString(o,sb);
        }else {
            sb.append(o);
        }
    }
    public static void out(Object o) {
        System.out.println(toAString(o));
    }

    private static void arrayToString(Object o,StringBuilder sb) {
        sb.append("[");
        boolean first=true;
        if(o instanceof String[]) {
            String[] strings=(String[])o;
            for(String s : strings) {
                if(first){
                    first=false;
                }else {
                    sb.append(',');
                }
                sb.append(s);
            }
        }
        else if (o instanceof char[]) {
            char[] chars=(char[])o;
            for(char c:chars) {
               sb.append(c);
            }
        }
        else if(o instanceof double[]) {
            double[] doubles=(double[])o;
            for(double s : doubles){
                if(first){
                    first=false;
                }else {
                    sb.append(',');
                }
                sb.append(s);
            }
        }
        else if(o instanceof int[]) {
            int[] ints=(int[])o;
            for(int s : ints){
                if(first){
                    first=false;
                }else {
                    sb.append(',');
                }
                sb.append(s);
            }
        }
        else if(o instanceof boolean[]) {
            boolean[] booleans=(boolean[])o;
            for(boolean s : booleans){
                if(first){
                    first=false;
                }else {
                    sb.append(',');
                }
                sb.append(s);
            }
        }
        else if(o instanceof Object[]) {
            Object[] objects=(Object[])o;
            for(Object ob : objects){
                if(first){
                    first=false;
                }else {
                    sb.append(',');
                }
                sb.append(toAString(ob));
            }
        }
        sb.append("]");
    }


    public static int sgn(double x) {
        if(x > 0)
            return 1;
        else if(x<0)
            return -1;
        else
            return 0;
    }
    public static void sleep(long time) {
        if (time<=0)
            return;
        try {
            Thread.sleep(time);
        }catch(Exception e) {}
    }
    public static List<Integer> number2Digits(int num){
        return number2Digits(num,10);
    }

    public static List<Integer> number2Digits(int num,int radix){
        if(radix <2 || radix >36)
            throw new RuntimeException("wrong parameter in number2digits: radix = "+radix);
        if(num < 0)
            throw new RuntimeException("wrong parameter in number2digits:only non-negative number" +
                    "supported");
        List<Integer> result=new ArrayList<Integer>();
        if (num == 0){
            result.add(0);
        }else {
            while(num > 0){
                result.add(num % radix);
                num/=radix;
            }
        }
        return result;
    }
    public static int digits2number(List<Integer> digits) {
        return  digits2number(digits,10);
    }
    public static int digits2number(List<Integer> digits,int radix)
    {
        if(radix <2 || radix >36)
            throw new RuntimeException("wrong parameter in digits2number: radix = "+radix);
        int result=0;
        for(int i=digits.size()-1;i>=0;i--){
            result=result * radix +digits.get(i);
        }
        return result;
    }
    public static int char2int(char c){
        if(c>='A' && c <= 'Z')
            c=(char)(c-('A'-'a'));
        if( '0'<= c && c <='9')
            return c-'0';
        else if(c>='a' && c<='z')
            return  (c-'a'+10);
        else
            throw new RuntimeException("wrong parameter in char2int: c = "+c);
    }
    public static char int2Char(int i){
        if( 0<= i && i <10)
            return (char)('0'+i);
        else if(i>=0 && i<36)
            return  (char)('a'+i-10);
        else
            throw new RuntimeException("wrong parameter in int2Char: i = "+i);
    }
    public static int gcd(int x, int y) {
        return x<y?gcd(y,x):(y == 0 ?x : gcd(y, x % y));
    }
    public static int[] getLowerLetterCount(String s){
        int[] result=new int[26];
        int length=s.length();
        for(int i=0;i<length;i++){
            result[s.charAt(i)-'a']++;
        }
        return result;
    }
    private static Random random=new Random();
    public static int[] randomIntegerArray(int low,int high,int length){
        int[] array=new int[length];
        int gap=high-low+1;
        for(int i=0;i<length;i++){
            array[i]=low+random.nextInt(gap);
        }
        return array;
    }
    public static String randomString(char low,char high,int length){
        StringBuilder sb=new StringBuilder();
        int[] array=randomIntegerArray(low,high,length);
        for(int x :array){
            sb.append((char)x);
        }
        return sb.toString();
    }
    public static String randomString(int length){
        return randomString((char)0,(char)255,length);
    }
    public static HashMap<Integer,Integer> getNumberCount(int[] array){
        HashMap<Integer,Integer> map=new HashMap<>();
        for(int x :array){
            int count=1+map.getOrDefault(x,0);
            map.put(x,count);
        }
        return map;
    }
    public static HashMap<Integer,Integer> getNumberCount(List<Integer> array){
        HashMap<Integer,Integer> map=new HashMap<>();
        for(int x :array){
            int count=1+map.getOrDefault(x,0);
            map.put(x,count);
        }
        return map;
    }
    public static boolean isPrime(int x){
        int sqrt=(int)Math.sqrt(x);
        for(int i=2;i<=sqrt;i++){
            if(x % i == 0)
                return false;
        }
        return true;
    }
    public static boolean isSymmetric(int x){
        if(x<0)
            return false;
        String s=String.valueOf(x);
        return s.equals(new StringBuilder(s).reverse().toString());
    }
    public static int mod(int x,int y){
        return x-Math.floorDiv(x,y)*y;
    }
    public static List<Integer> parseIntegerList(String string,String delimiter){
        List<Integer> result=new ArrayList<>();
        String[] parts=string.split(delimiter);
        for(String part : parts){
            result.add(Integer.parseInt(part));
        }
        return result;
    }
    public static boolean toFile(String content,String filePath){
        File file=new File(filePath);
        try {
            BufferedWriter writer=new BufferedWriter(new OutputStreamWriter
                    (new FileOutputStream(file),"UTF-8"));
            String line;
            writer.write(content,0,content.length());
            writer.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    public static String fromFile(String filePath){
        File inputFile=new File(filePath);
        if(!inputFile.exists())
            return null;
        try {
            FileInputStream fis=new FileInputStream(inputFile);
            InputStreamReader isr=new InputStreamReader(fis, "utf-8");
            BufferedReader reader = new BufferedReader(isr);
            StringBuilder sb=new StringBuilder();
            String line;
            while ((line=reader.readLine())!=null){
                sb.append(line).append('\n');
            }
            reader.close();
            return sb.toString();
        } catch (Exception e) {
            return null;
        }
    }

    public static void removeLastFromList(List list){
        if(list!=null && list.size()>0){
            list.remove(list.size()-1);
        }
    }
    public static void outListNoBracket(List<Integer> list,StringBuilder sb){
        for (int i = 0; i < list.size(); i++) {
            if(i>0)
                sb.append(",");
            sb.append(list.get(i));
        }
    }
    public static void clearDirectories(String[] directories){
        for(String directoryPath:directories){
            File directory=new File(directoryPath);
            File[] files=directory.listFiles();
            if (files!=null){
                for(File file:files){
                    file.delete();
                }
            }
        }
    }

    public static void listEachElementPlus(List<Integer> list,int plus){
        for (int i = 0; i < list.size(); i++) {
            list.set(i,list.get(i)+plus);
        }
    }

    public static void fileCopy(String source, String dest){

        FileChannel inputChannel = null;
        FileChannel outputChannel = null;
        try {
            inputChannel = new FileInputStream(source).getChannel();
            outputChannel = new FileOutputStream(dest).getChannel();
            outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            try {
                if(inputChannel!=null)
                    inputChannel.close();
                if(outputChannel!=null)
                    outputChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings("")
    public static boolean equall(List<Integer> list1,List<Integer> list2){
        if(list1==null && list2==null){
            return true;
        }
        if(list1==null || list2==null){
            return true;
        }
        if(list1.size()!=list2.size())
            return false;
        for (int i = 0; i < list1.size(); i++) {
            if(!list1.get(i).equals(list2.get(i)))
                return false;
        }
        return true;
    }

    public static boolean equalll(List<List<Integer>> ll1,List<List<Integer>> ll2){
        if(ll1==null && ll2==null){
            return true;
        }
        if(ll1==null || ll2==null){
            return true;
        }
        if(ll1.size()!=ll2.size())
            return false;
        for (int i = 0; i < ll1.size(); i++) {
            if(!Util.equall(ll1.get(i),ll2.get(i)))
                return false;
        }
        return true;
    }

    public static int partition(int[] array, int low, int high){
        int temp=array[low];
        while (low<high){
            while (low<high && array[high]>=temp)
                high--;
            array[low]=array[high];
            while (low<high && array[low]<=temp)
                low++;
            array[high]=array[low];
        }
        array[low]=temp;
        return low;
    }

    public static List<Integer> range(int count){
        List<Integer> result=new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(i);
        }
        return result;
    }

    public static String excelStyleColumnName(int x){
        StringBuilder sb=new StringBuilder();
        while (x!=0){
            x--;
            sb.append((char)('A'+ x % 26));
            x=x/26;
        }
        return sb.reverse().toString();
    }

    public static Collection<Integer> randomSample(int totalCount,int resultLength){
        List<Integer> list=new ArrayList<>(totalCount);
        for (int i = 0; i < totalCount; i++) {
            list.add(i);
        }
        if (resultLength>totalCount){
            return list;
        }
        Collections.shuffle(list,random);
        return list.subList(0,resultLength);
    }

}
