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
//        String s = "D:\\zkr\\项目\\大数据\\doc\\06.项目开发\\01.数据平台\\ETL脚本\\建表脚本\\ods\\reldata\\cmsp\\dt\\";
//        File inputFilePath = new File(s);
//        for (File inputFile : inputFilePath.listFiles()){
//            System.out.println(inputFile.getName());
//        }



//        SQL_resolver sql_resolver = new SQL_resolver();


        TablePOJO a = new TablePOJO();
        a.setTableName("njy1");

        TablePOJO b = new TablePOJO();
        b.setTableName("njy2");

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
}
