import SQL_resolver.POJO.JoinPOJO;
import SQL_resolver.POJO.JoinTable;
import SQL_resolver.POJO.TablePOJO;
import SQL_resolver.Resolver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class test {
    public static void main(String[] args) {
        test2();


    }

    public static void test1() {
        String s = "D:\\zkr\\项目\\大数据\\doc\\06.项目开发\\01.数据平台\\ETL脚本\\建表脚本\\ods\\reldata\\cmsp\\dt\\";
        File inputFilePath = new File(s);
        for (File inputFile : inputFilePath.listFiles()) {
            System.out.println(inputFile.getName());
        }
    }

    public static void test2() {
        Resolver sql_resolver = new Resolver();
    }

    public static void test3() {
        TablePOJO a = new TablePOJO(1, "njy1");

        TablePOJO b = new TablePOJO(2, "njy2");

        JoinTable l = new JoinTable();
        l.setJoinTablePOJO(b);

        List<String> c = new ArrayList<>();
        c.add("haha");

        l.setJoinCondition(c);

        List<JoinTable> lList = new ArrayList<>();
        lList.add(l);

        a.setJoinTableList(lList);

        System.out.println(b.getJoinConditionWith(a));
    }

    public static void test4() {
        TablePOJO a = new TablePOJO(1, "njy1");

        TablePOJO b = new TablePOJO(2, "njy2");

        TablePOJO c = new TablePOJO(3, "njy3");

        List<String> list1 = new ArrayList<>();
        list1.add("haha1");

        List<String> list2 = new ArrayList<>();
        list2.add("haha2");

        JoinPOJO lab = new JoinPOJO(a, b, list1);
        JoinPOJO lbc = new JoinPOJO(b, c, list2);


        System.out.println(a.getJoinPOJOListTo(c));
    }

    //测试 subList 同步更新
    private void test5(){
        List<String> wordList = new ArrayList<>();
        wordList.add("1");
        wordList.add("2");
        wordList.add("3");
        System.out.println();
        System.out.println(wordList);
        List<String> subList = wordList.subList(1,3);
        System.out.println(subList);
        subList.add("4");
        System.out.println(subList);
        System.out.println(wordList);
    }
}
