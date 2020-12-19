import ToolWorkSpace.SQL_resolver.POJO.LinkPOJO;
import ToolWorkSpace.SQL_resolver.POJO.LinkTable;
import ToolWorkSpace.SQL_resolver.POJO.TablePOJO;

import javax.swing.filechooser.FileSystemView;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class test {
    public static void main(String[] args) {
        test5();
    }

    public static void test1() {
        String s = "D:\\zkr\\项目\\大数据\\doc\\06.项目开发\\01.数据平台\\ETL脚本\\建表脚本\\ods\\reldata\\cmsp\\dt\\";
        File inputFilePath = new File(s);
        for (File inputFile : inputFilePath.listFiles()) {
            System.out.println(inputFile.getName());
        }
    }

    public static void test2() {
        SQL_resolver sql_resolver = new SQL_resolver();
    }


    public static void test3() {
        TablePOJO a = new TablePOJO(1, "njy1");

        TablePOJO b = new TablePOJO(2, "njy2");

        LinkTable l = new LinkTable();
        l.setLinkTablePOJO(b);

        List<String> c = new ArrayList<>();
        c.add("haha");

        l.setLinkCondition(c);

        List<LinkTable> lList = new ArrayList<>();
        lList.add(l);

        a.setLinkTableList(lList);

        System.out.println(b.getLinkConditionWith(a));
    }

    public static void test4() {
        TablePOJO a = new TablePOJO(1, "njy1");

        TablePOJO b = new TablePOJO(2, "njy2");

        TablePOJO c = new TablePOJO(3, "njy3");

        List<String> list1 = new ArrayList<>();
        list1.add("haha1");

        List<String> list2 = new ArrayList<>();
        list2.add("haha2");

        LinkPOJO lab = new LinkPOJO(a, b, list1);
        LinkPOJO lbc = new LinkPOJO(b, c, list2);


        System.out.println(a.getLinkPOJOWith(c));
    }

    public static void test5() {
        TablePOJO a = new TablePOJO(1, "njy1");

        TablePOJO b = new TablePOJO(2, "njy2");

        TablePOJO c = new TablePOJO(3, "njy3");

        List<String> list1 = new ArrayList<>();
        list1.add("haha1");

        List<String> list2 = new ArrayList<>();
        list2.add("haha2");

        LinkPOJO lab = new LinkPOJO(a, b, list1);
        LinkPOJO lbc = new LinkPOJO(b, c, list2);


        System.out.println(a.getLinkPOJOListTo(c));
    }
}
