import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQL_resolver {
    private String main_table = null;
    private String main_table_alias = null;
    private String sub_table = null;
    private String sub_table_alias = null;
    private int index = 0;
    private String sql = null;

    public SQL_resolver() {


        try {
            setSql();
            function2(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("主表：" + main_table);
        System.out.println("主表别名：" + main_table_alias);
        System.out.println("子表：" + sub_table);
        System.out.println("子表别名：" + sub_table_alias);

    }


    private String function1(String subSql, int subIndex) {
        StringBuilder word = new StringBuilder("");
        String tempstr = null;
        String tempstr2 = null;
        for (int i = subIndex; i < subSql.length(); i++) {
            tempstr = subSql.substring(i, i + 1);
            if (!tempstr.matches("\\s")) {
                word.append(tempstr);
                continue;
            }


            break;


//            tempstr2=word.toString();
//            if(tempstr2.toUpperCase().equals("SELECT")){
//
//            }
//            if(tempstr2.toUpperCase().equals("FROM")){
//
//            }
//            System.out.println(word.toString());


        }
//        System.out.println(word.toString());
        return word.toString();
    }

    private void function2(String subSql) {
        int j = 0;

        String temp1 = "";
//        String temp1 = function1(sql, index);

        //SELECT
        do {
            temp1 = function1(sql, index + temp1.length() + j);
            j = 1;
            index += temp1.length() + j;
            System.out.println(temp1);
        } while (!temp1.toUpperCase().equals("SELECT"));

        //FROM
        do {
            temp1 = function1(sql, index);
            index += temp1.length() + 1;
            System.out.println(temp1);
        } while (!temp1.toUpperCase().equals("FROM"));


        //JOIN
        do {
            temp1 = function1(sql, index);
            index += temp1.length() + 1;
            System.out.println(temp1);

            //MAIN_TABLE
            if (main_table_alias == null){
                if (main_table == null){
                    if (temp1.matches("soochow_data\\.(\\w)+")) {
                        main_table = getMatchStrs(temp1, "soochow_data\\.(\\w)+").get(0);
                    }
                }else{
                    if (temp1.matches("\\w+")) {
                        main_table_alias = temp1;
                    }
                }
            }


        } while (!temp1.toUpperCase().equals("JOIN"));

//        join();
//        where();

    }


    public String setSql() throws Exception {
        File testFilePath = new File("D:\\Development\\ideaWorkSpace\\Java-Tools\\src\\ToolWorkSpace\\SQL_resolver\\test");
        File testFile = testFilePath.listFiles()[0];

        StringBuilder sqlStringBuilder = new StringBuilder("");
        String lineTxt = null;

        InputStreamReader templateStreamReader = new InputStreamReader(new FileInputStream(testFile), "UTF-8");
        BufferedReader templateBufferReader = new BufferedReader(templateStreamReader);
        while ((lineTxt = templateBufferReader.readLine()) != null) {
            sqlStringBuilder.append(lineTxt).append("\n");
        }
        templateStreamReader.close();
        this.sql = sqlStringBuilder.toString();
        return sqlStringBuilder.toString();
    }


    public List<String> getMatchStrs(String str, String reg) {
        Pattern patten = Pattern.compile(reg);//编译正则表达式
        Matcher matcher = patten.matcher(str);// 指定要匹配的字符串

        List<String> matchStrs = new ArrayList<>();

        while (matcher.find()) { //此处find()每次被调用后，会偏移到下一个匹配
            matchStrs.add(matcher.group());//获取当前匹配的值
        }
        return matchStrs;
    }
}


