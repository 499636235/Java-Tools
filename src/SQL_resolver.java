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
            resolveOneSql2(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    //目前的功能：获取subSql中 从下标subIndex处开始的 一个不含"\s"(空字符)的字符串
    private String getNotNullWord(String subSql, int subIndex) {
        StringBuilder word = new StringBuilder("");
        String tempstr = null;
        for (int i = subIndex; i < subSql.length(); i++) {
            tempstr = subSql.substring(i, i + 1);
            if (!tempstr.matches("\\s")) {
                word.append(tempstr);
                continue;
            }
            break;
        }
        return word.toString();
    }

    private void resolveOneSql(String subSql) {
        int j = 0;
        String temp1 = "";


        //SELECT
        do {
            temp1 = getNotNullWord(sql, index);
            index += temp1.length() + 1;
            System.out.println(temp1);
        } while (!temp1.toUpperCase().equals("SELECT") && index < subSql.length());


        //FROM
        do {
            temp1 = getNotNullWord(sql, index);
            index += temp1.length() + 1;
            System.out.println(temp1);
        } while (!temp1.toUpperCase().equals("FROM") && index < subSql.length());


        //JOIN
        do {
            temp1 = getNotNullWord(sql, index);
            index += temp1.length() + 1;
            System.out.println(temp1);

            //MAIN_TABLE
            if (main_table_alias == null) {
                if (main_table == null) {
                    if (temp1.matches("soochow_data\\.(\\w)+")) {
                        main_table = getMatchStrs(temp1, "soochow_data\\.(\\w)+").get(0);
                    }
                } else {
                    if (temp1.matches("\\w+")) {
                        main_table_alias = temp1;
                    }
                }
            }

        } while (!temp1.toUpperCase().equals("JOIN") && index < subSql.length());


        //ON
        do {
            temp1 = getNotNullWord(sql, index);
            index += temp1.length() + 1;
            System.out.println(temp1);

            //SUB_TABLE
            if (sub_table_alias == null) {
                if (sub_table == null) {
                    if (temp1.matches("soochow_data\\.(\\w)+")) {
                        sub_table = getMatchStrs(temp1, "soochow_data\\.(\\w)+").get(0);
                    }
                } else {
                    if (temp1.matches("\\w+")) {
                        sub_table_alias = temp1;
                    }
                }
            }

        } while (!temp1.toUpperCase().equals("ON") && index < subSql.length());

        //关联条件开始处下标
        int linkStart = index;

        //WHERE
        do {
            temp1 = getNotNullWord(sql, index);
            index += temp1.length() + 1;
            System.out.println(temp1);


        } while (!temp1.toUpperCase().equals("WHERE") && index < subSql.length());

        //关联条件结束处下标
        int linkEnd = index - 6;

        System.out.println("关联条件:" + sql.substring(linkStart, linkEnd));

    }

    private void resolveOneSql2(String subSql) {
        int j = 0;
        String temp1 = "";
        String[] keywordArray = {"SELECT", "FROM", "JOIN", "ON", "WHERE", ""};
        int keywordIndex = 0;

        List<String> wordList = new ArrayList<>();
        int linkStart = 0;
        int linkEnd = 0;

        while (index < subSql.length() && keywordIndex < keywordArray.length) {
            temp1 = getNotNullWord(sql, index);
            index += temp1.length() + 1;

            wordList.add(temp1);

            // 找关键字 只有找到当前关键字才能找下一个关键字
            if (temp1.toUpperCase().equals(keywordArray[keywordIndex])) {
                keywordIndex++;
                continue;
            }

            // FROM 与 JOIN 之间为 MAIN_TABLE
            if (keywordIndex == 2) {
                //MAIN_TABLE
                if (main_table_alias == null) {
                    if (main_table == null) {
                        if (temp1.matches("soochow_data\\.(\\w)+")) {
                            main_table = getMatchStrs(temp1, "soochow_data\\.(\\w)+").get(0);
                        }
                    } else {
                        if (temp1.matches("\\w+")) {
                            main_table_alias = temp1;
                        }
                    }
                }
            }

            // JOIN 与 ON 之间为 SUB_TABLE
            if (keywordIndex == 3) {
                //SUB_TABLE
                if (sub_table_alias == null) {
                    if (sub_table == null) {
                        if (temp1.matches("soochow_data\\.(\\w)+")) {
                            sub_table = getMatchStrs(temp1, "soochow_data\\.(\\w)+").get(0);
                        }
                    } else {
                        if (temp1.matches("\\w+")) {
                            sub_table_alias = temp1;
                        }
                    }
                }
            }

            // ON 与 WHERE 之间为 关联条件
            if (keywordIndex == 4 && linkStart == 0) {
                //关联条件开始处下标
                linkStart = index - temp1.length() - 1;
            }

            // WHERE 之后的部分 暂时忽略
            if (keywordIndex == 5 && linkEnd == 0) {
                //关联条件结束处下标
                linkEnd = index - temp1.length() - 1;
            }

        }

        System.out.println(wordList);
        System.out.println("主表：" + main_table);
        System.out.println("主表别名：" + main_table_alias);
        System.out.println("子表：" + sub_table);
        System.out.println("子表别名：" + sub_table_alias);
        System.out.println("关联条件:" + sql.substring(linkStart, linkEnd));

    }


    //从文件读取SQL并赋值给全局变量:sql
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


    //从str中找到匹配正则表达式reg的字符串list
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


