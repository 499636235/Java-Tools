package SQL_resolver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL解析器(开发中)
 * 目标是解析SQL中的所有表之间的关联条件
 */
public class Resolver {

    private String main_table = null;

    private String main_table_alias = null;

    private String sub_table = null;

    private String sub_table_alias = null;

    /**
     * 读取整段SQL时使用的游标
     */
    private int index = 0;

    /**
     * 需要读取的整段SQL
     */
    private String sql = null;


    /**
     * 启动器 (调试用)
     */
    public Resolver() {
        try {
            setSql();
            resolveOneSql2(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 目前的功能：获取subSql中 从下标subIndex处开始的 一个不含"\s"(空字符)的字符串
     *
     * @param subSql
     * @param subIndex
     * @return
     */
    private String getWholeWord(String subSql, int subIndex) {
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

    /**
     * 解析一段只有一个SELECT的SQL (Demo1)
     *
     * @param subSql
     */
//    private void resolveOneSql(String subSql) {
//        int j = 0;
//        String temp1 = "";
//
//
//        //SELECT
//        do {
//            temp1 = getWholeWord(sql, index);
//            index += temp1.length() + 1;
//            System.out.println(temp1);
//        } while (!temp1.toUpperCase().equals("SELECT") && index < subSql.length());
//
//
//        //FROM
//        do {
//            temp1 = getWholeWord(sql, index);
//            index += temp1.length() + 1;
//            System.out.println(temp1);
//        } while (!temp1.toUpperCase().equals("FROM") && index < subSql.length());
//
//
//        //JOIN
//        do {
//            temp1 = getWholeWord(sql, index);
//            index += temp1.length() + 1;
//            System.out.println(temp1);
//
//            //MAIN_TABLE
//            if (main_table_alias == null) {
//                if (main_table == null) {
//                    if (temp1.matches("soochow_data\\.(\\w)+")) {
//                        main_table = getMatchStrs(temp1, "soochow_data\\.(\\w)+").get(0);
//                    }
//                } else {
//                    if (temp1.matches("\\w+")) {
//                        main_table_alias = temp1;
//                    }
//                }
//            }
//
//        } while (!temp1.toUpperCase().equals("JOIN") && index < subSql.length());
//
//
//        //ON
//        do {
//            temp1 = getWholeWord(sql, index);
//            index += temp1.length() + 1;
//            System.out.println(temp1);
//
//            //SUB_TABLE
//            if (sub_table_alias == null) {
//                if (sub_table == null) {
//                    if (temp1.matches("soochow_data\\.(\\w)+")) {
//                        sub_table = getMatchStrs(temp1, "soochow_data\\.(\\w)+").get(0);
//                    }
//                } else {
//                    if (temp1.matches("\\w+")) {
//                        sub_table_alias = temp1;
//                    }
//                }
//            }
//
//        } while (!temp1.toUpperCase().equals("ON") && index < subSql.length());
//
//        //关联条件开始处下标
//        int linkStart = index;
//
//        //WHERE
//        do {
//            temp1 = getWholeWord(sql, index);
//            index += temp1.length() + 1;
//            System.out.println(temp1);
//
//
//        } while (!temp1.toUpperCase().equals("WHERE") && index < subSql.length());
//
//        //关联条件结束处下标
//        int linkEnd = index - 6;
//
//        System.out.println("关联条件:" + sql.substring(linkStart, linkEnd));
//
//    }

    /**
     * 解析一段只有一个SELECT的SQL (Demo2)
     *
     * @param subSql
     */
    private void resolveOneSql2(String subSql) {
        String temp1 = "";//临时字符串对象

        String[] keywordArray = {"SELECT", "FROM", "JOIN", "ON", "WHERE", "END"};//关键字数组

        int keywordIndex = 0;//下一个要匹配的关键字(对应的下标)

        List<String> wordList = new ArrayList<>();//存放 SQL中的每个整词 的 List

        List<Integer> wordIndexList = new ArrayList<>();//SQL中的每个整词 起始处 所对应的下标

        int linkStart = 0;
        int linkEnd = 0;

        while (index < subSql.length()) {
            temp1 = getWholeWord(sql, index);//获取下一个完整的词
            if (!temp1.equals("")) {//过滤空字符
                wordList.add(temp1);//把整词加入List
                wordIndexList.add(index);//把整词起始处的下标加入List
                index += temp1.length();//游标推进
            } else {
                index += 1;
            }
        }

        int i = 0;
        while (keywordIndex < keywordArray.length && i < wordList.size()) {

            temp1 = wordList.get(i);

            // FROM 与 JOIN 之间为 MAIN_TABLE
            if (keywordArray[keywordIndex].equals("JOIN")) {
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
            if (keywordArray[keywordIndex].equals("ON")) {
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
            if (keywordArray[keywordIndex].equals("WHERE") && linkStart == 0) {
                //关联条件开始处下标
                linkStart = i;
            }

            // WHERE 之后的部分 暂时忽略
            if (keywordArray[keywordIndex].equals("END") && linkEnd == 0) {
                //关联条件结束处下标
                linkEnd = i - 2;
            }


            // 匹配关键字 只有当前关键字匹配成功时 才能继续匹配下一个关键字
            if (temp1.toUpperCase().equals(keywordArray[keywordIndex])) {
                keywordIndex++;
            }
            i++;
        }

        System.out.println(wordList);
        System.out.println(wordIndexList);
        System.out.println("主表：" + main_table);
        System.out.println("主表别名：" + main_table_alias);
        System.out.println("子表：" + sub_table);
        System.out.println("子表别名：" + sub_table_alias);
        System.out.println("关联条件:");
        for (int linkIndex = linkStart; linkIndex <= linkEnd; linkIndex++) {
            System.out.print(wordList.get(linkIndex) + " ");
        }


    }

    /**
     * 从文件读取SQL并赋值给全局变量:sql
     *
     * @return
     * @throws Exception
     */
    public String setSql() throws Exception {
        File testFilePath = null;

        testFilePath = new File("D:\\Development\\ideaWorkSpace\\Java-Tools\\src\\ToolWorkSpace\\SQL_resolver\\test");
        if (testFilePath.listFiles() == null) {
            testFilePath = new File("D:\\Development\\ideaWorkplace\\Java-Tools\\src\\ToolWorkSpace\\SQL_resolver\\test");
        }
        File testFile = testFilePath.listFiles()[0];

        StringBuilder sqlStringBuilder = new StringBuilder("");
        String lineTxt = null;

        InputStreamReader templateStreamReader = new InputStreamReader(new FileInputStream(testFile), "UTF-8");
        BufferedReader templateBufferReader = new BufferedReader(templateStreamReader);
        while ((lineTxt = templateBufferReader.readLine()) != null) {
            lineTxt.replaceAll("\\("," \\( ");
            lineTxt.replaceAll("\\)"," \\) ");
            sqlStringBuilder.append(lineTxt).append("\n");
        }
        templateStreamReader.close();
        this.sql = sqlStringBuilder.toString();
        return sqlStringBuilder.toString();
    }

    /**
     * 从str中找到匹配正则表达式reg的字符串list
     *
     * @param str
     * @param reg
     * @return
     */
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

