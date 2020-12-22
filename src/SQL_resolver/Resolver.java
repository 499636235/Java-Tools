package SQL_resolver;

import SQL_resolver.POJO.WordListInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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
            resolveWhileSql(sql);
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
     * 把 SQL 转换成 整词List
     *
     * @param subSql
     * @return
     */
    public List<String> getWordList(String subSql) {
        // 临时字符串对象
        String temp1 = "";
        // 存放 SQL中的每个整词 的 List
        List<String> wordList = new ArrayList<>();
        // SQL中的每个整词 起始处 所对应的下标
        List<Integer> wordIndexList = new ArrayList<>();

        // 推进游标直到SQL结束
        while (index < subSql.length()) {
            // 获取下一个完整的词
            temp1 = getWholeWord(sql, index);
            // 过滤空字符
            if (!temp1.equals("")) {
                //把整词加入List
                wordList.add(temp1);
                // 把整词起始处的下标加入List
                wordIndexList.add(index);
                // 游标推进
                index += temp1.length();
            } else {
                // 跳过空字符
                index += 1;
            }
        }

        // 测试输出
        String[] keywordArray = {"SELECT", "FROM", "JOIN", "ON", "WHERE", "AND"};
        List<String> keywordList = Arrays.asList(keywordArray);
        for (String s : wordList) {
            if (keywordList.contains(s.toUpperCase())) {
                System.out.print("\n");
            }
            System.out.print(s + " ");

        }
        System.out.println();
        System.out.println();

        return wordList;
    }

    /**
     * 解析整段SQL
     *
     * @param subSql
     */
    public void resolveWhileSql(String subSql) {
        // 获取 SQL 转换成的 整词List
        List<String> wordList = getWordList(subSql);
        // 递归解析SQL
        WordListInfo returnWordListInfo = resolveOneSql2(wordList);

        // 测试输出
        System.out.println("递归解析SQL:" + returnWordListInfo.getWordList() + "\n");


    }

    /**
     * 解析一段只有一个SELECT的SQL (Demo2)
     * 理想设计是递归
     *
     * @param wordList
     * @return int
     */
    private WordListInfo resolveOneSql2(List<String> wordList) {
        // 当前 wordList 信息
        WordListInfo wordListInfo = new WordListInfo();
        // 临时字符串对象
        String temp1 = "";
        // 关键字数组
        String[] keywordArray = {"SELECT", "FROM", "JOIN", "ON", "WHERE", "END"};
        // 下一个要匹配的关键字(在 keywordArray 中对应的下标)
        int keywordIndex = 0;
        // 关联条件开始处下标
        int joinStart = 0;
        // 关联条件结束处下标
        int joinEnd = 0;
        // 当前整词在 wordList 中的下标
        int currentWordIndex = 0;
        // 标志当前循环中是否已经判断过 wordList 是否为SQL查询
        boolean ifJudgeSqlFlag = false;
        // 分割关键词 开始的位置 (用于捕获)
        int splitKeywordStart = 0;
        // 分割关键词 结束的位置 (用于捕获)
        int splitKeywordEnd = 0;
        // 分割关键词 List
        List<String> splitKeywordList = Arrays.asList(new String[]{"FROM", "JOIN", "ON", "WHERE", ")"});

        while (keywordIndex < keywordArray.length && currentWordIndex < wordList.size()) {
            // 当前整词
            temp1 = wordList.get(currentWordIndex);
            // 如果是左括号则进入递归
            if (temp1.equals("(")) {
                // 参数为该左括号之后的sql
                List<String> subList = wordList.subList(currentWordIndex + 1, wordList.size());

                // 返回子SQL的整词个数
                WordListInfo returnWordListInfo = resolveOneSql2(subList);

                // 如果是SQL查询
                if (returnWordListInfo.getIfSqlFlag()) {
                    // 递归解析的 子查询
                    System.out.println("递归解析的子查询:" + returnWordListInfo.getWordList() + "\n");





                    // TODO 处理前面括号中的内容，判断是否为一张表，如果是表怎么关联？
                    // 这个List长度一定要先赋值给临时变量，否则报错！
                    int deleteNum = returnWordListInfo.getWordList().size() + 1;
                    // 移除子查询以及左括号
                    for (int i = 0; i < deleteNum; i++) {
                        wordList.remove(currentWordIndex);
                    }
                    //把剩下的右括号替换成新的表名
                    wordList.set(currentWordIndex, "njy");

                } else {
                    // +2 是因为有左右括号
                    int jumpWordNum = returnWordListInfo.getWordList().size() + 2;
                    // 让上层递归跳过括号中的内容
                    currentWordIndex += jumpWordNum;
                }


                continue;

            }

            // 每对括号中只用判断一次
            if (!ifJudgeSqlFlag) {
                // 如果第一个词不是 SELECT
                if (!wordList.get(0).toUpperCase().equals("SELECT")) {
                    // 不是SQL查询
                    wordListInfo.setIfSqlFlag(false);
                }
                // 已经判断过 ifSqlFlag 的真假
                ifJudgeSqlFlag = true;
            }


            // 如果是SQL查询
            if (wordListInfo.getIfSqlFlag()) {

                // 如果当前整词是分割关键词之一
                if (splitKeywordList.contains(temp1.toUpperCase())) {
                    // 如果 当前整词是 FROM
                    if (temp1.toUpperCase().equals("FROM")) {
                        // 记录当前分割关键词下标为捕获开始处
                        splitKeywordStart = currentWordIndex;
                    }

                    // 如果 上一个分割关键词是 FROM 并且 当前整词是 JOIN
                    if (wordList.get(splitKeywordStart).toUpperCase().equals("FROM") && temp1.toUpperCase().equals("JOIN")) {
                        // 记录当前分割关键词下标为捕获结束处
                        splitKeywordEnd = currentWordIndex;
                        System.out.println("捕获到" + wordList.get(splitKeywordStart) + "和" + wordList.get(splitKeywordEnd) + "之间的内容:" + wordList.subList(splitKeywordStart + 1, splitKeywordEnd));
                        splitKeywordStart = currentWordIndex;
                    }

                    // 如果 上一个分割关键词是 JOIN 并且 当前整词是 ON
                    if (wordList.get(splitKeywordStart).toUpperCase().equals("JOIN") && temp1.toUpperCase().equals("ON")) {
                        // 记录当前分割关键词下标为捕获结束处
                        splitKeywordEnd = currentWordIndex;
                        System.out.println("捕获到" + wordList.get(splitKeywordStart) + "和" + wordList.get(splitKeywordEnd) + "之间的内容:" + wordList.subList(splitKeywordStart + 1, splitKeywordEnd));
                        splitKeywordStart = currentWordIndex;
                    }

                    // 如果 上一个分割关键词是 ON 并且 当前整词是 JOIN
                    if (wordList.get(splitKeywordStart).toUpperCase().equals("ON") && (temp1.toUpperCase().equals("JOIN") || temp1.toUpperCase().equals("WHERE"))) {
                        // 记录当前分割关键词下标为捕获结束处
                        splitKeywordEnd = currentWordIndex;
                        System.out.println("捕获到" + wordList.get(splitKeywordStart) + "和" + wordList.get(splitKeywordEnd) + "之间的内容:" + wordList.subList(splitKeywordStart + 1, splitKeywordEnd));
                        splitKeywordStart = currentWordIndex;
                    }

                    // 如果 当前整词是 ")"
                    if (temp1.equals(")")) {
                        // 记录当前分割关键词下标为捕获结束处
                        splitKeywordEnd = currentWordIndex;
                        System.out.println("捕获到" + wordList.get(splitKeywordStart) + "和" + wordList.get(splitKeywordEnd) + "之间的内容:" + wordList.subList(splitKeywordStart + 1, splitKeywordEnd));
                        splitKeywordStart = currentWordIndex;
                    }


                }
            } else {
                // 如果不是SQL查询，当遇到右括号时退出递归
                if (temp1.equals(")")) {
                    break;
                }
                currentWordIndex++;
                continue;
            }

            // 当遇到右括号时退出递归，并且返回子SQL的整词个数，让上层递归跳过子SQL
            if (temp1.equals(")")) {

                // TODO 如果当前递归 从这个判断中结束，一定是SQL查询，所以在 break 之前要完成返回值的赋值（返回值应该包含很多信息而不只是一个int）


                break;
            }


/*

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
                // SUB_TABLE
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
            if (keywordArray[keywordIndex].equals("WHERE") && joinStart == 0) {
                //关联条件开始处下标
                joinStart = currentWordIndex;
            }

            // WHERE 之后的部分 暂时忽略
            if (keywordArray[keywordIndex].equals("END") && joinEnd == 0) {
                // 关联条件结束处下标
                joinEnd = currentWordIndex - 2;
            }
*/


            // 匹配关键字 只有当前关键字匹配成功时 才能继续匹配下一个关键字
            if (temp1.toUpperCase().equals(keywordArray[keywordIndex])) {
                keywordIndex++;
            }
            currentWordIndex++;
        }


        // TODO 把表转换成 TablePOJO

        // TODO 建立所有 TablePOJO 之间的 JoinPOJO

/*

        System.out.println();
        System.out.println("当前wordList:" + wordList.subList(0, currentWordIndex));
        System.out.println("主表：" + main_table);
        System.out.println("主表别名：" + main_table_alias);
        System.out.println("子表：" + sub_table);
        System.out.println("子表别名：" + sub_table_alias);
        System.out.print("关联条件：");
        for (int joinIndex = joinStart; joinIndex <= joinEnd; joinIndex++) {
            System.out.print(wordList.get(joinIndex) + " ");
        }
        System.out.println();
*/

        // 返回当前 wordListInfo
        wordListInfo.setWordList(wordList.subList(0, currentWordIndex));
        return wordListInfo;
    }

    /**
     * 从文件读取SQL并赋值给全局变量:sql
     *
     * @return
     * @throws Exception
     */
    public String setSql() throws Exception {
        File testFilePath = null;

        testFilePath = new File("src\\ToolWorkSpace\\SQL_resolver\\test");

        File testFile = testFilePath.listFiles()[0];

        StringBuilder sqlStringBuilder = new StringBuilder("");
        String lineTxt = null;

        InputStreamReader templateStreamReader = new InputStreamReader(new FileInputStream(testFile), "UTF-8");
        BufferedReader templateBufferReader = new BufferedReader(templateStreamReader);
        while ((lineTxt = templateBufferReader.readLine()) != null) {
            // 左括号两边加上空格，方便转换
            lineTxt = lineTxt.replaceAll("\\(", " \\( ");
            // 右括号两边加上空格，方便转换
            lineTxt = lineTxt.replaceAll("\\)", " \\) ");
            // 去除注释
            lineTxt = lineTxt.replaceAll("--.*", "");
            //把"select*from"拆开，方便转换
            lineTxt = lineTxt.replaceAll("(?i)select(\\s)?\\*(\\s)?from", "select \\* from");
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
        // 编译正则表达式
        Pattern patten = Pattern.compile(reg);
        // 指定要匹配的字符串
        Matcher matcher = patten.matcher(str);

        List<String> matchStrs = new ArrayList<>();

        // 此处find()每次被调用后，会偏移到下一个匹配
        while (matcher.find()) {
            // 获取当前匹配的值
            matchStrs.add(matcher.group());
        }
        return matchStrs;
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
//        int joinStart = index;
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
//        int joinEnd = index - 6;
//
//        System.out.println("关联条件:" + sql.substring(joinStart, joinEnd));
//
//    }
}


