import javax.swing.filechooser.FileSystemView;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class realdata_dwd_generator {
    public static void main(String[] args) throws Exception {
        realdata_dwd_generator.work();
    }

    private static boolean work() throws Exception {
        File desktopPath = FileSystemView.getFileSystemView().getHomeDirectory();
        File inputFilePath = new File(desktopPath + "\\input");
//        File inputFile = inputFilePath.listFiles()[0];
        File outputFilePath = new File(desktopPath + "\\output");;
        File outputFile = null;
        File templateFilePath = new File(desktopPath + "\\template");
        File templateFile = templateFilePath.listFiles()[0];
        File primaryKeyFilePath = new File(desktopPath + "\\primary key");
        File primaryKeyFile = primaryKeyFilePath.listFiles()[0];
        File file = null;
        File[] tempFileList = templateFilePath.listFiles();
        String fileName = null;
        String tableName = null;
        String primaryKeyColumn = null;
        String lineTxt = null;
        ArrayList<String> stringArrayListTemp = new ArrayList<>();
        StringBuilder stringBuilder1 = new StringBuilder("");
        StringBuilder stringBuilder2 = new StringBuilder("");
        StringBuilder stringBuilder3 = new StringBuilder("");
        int i = 0,j = 0,k = 0;
        HashMap<String,ArrayList<String>> primaryKeyMap = new HashMap<>();


        //读取模板文档
        InputStreamReader templateStreamReader = new InputStreamReader(new FileInputStream(templateFile), "UTF-8");
        BufferedReader templateBufferReader = new BufferedReader(templateStreamReader);
        while ( (j++==-1)||(lineTxt = templateBufferReader.readLine()) != null ) {
            if (lineTxt.startsWith("[partdivider]"))
            {
//                    stringBuilder1.append("\n");
                stringArrayListTemp.add(stringBuilder1.toString());
                stringBuilder1.delete(0, stringBuilder1.length());
                stringArrayListTemp.add(lineTxt);

                j=0;
            }else {
                if(j==1){
                    stringBuilder1.append(lineTxt);
                }else{
                    stringBuilder1.append("\n"+lineTxt);
                }
            }
        }
        templateStreamReader.close();

        //读取主键文档
        InputStreamReader primaryKeyStreamReader = new InputStreamReader(new FileInputStream(primaryKeyFile), "UTF-8");
        BufferedReader primaryKeyBufferReader = new BufferedReader(primaryKeyStreamReader);
        while ((lineTxt = primaryKeyBufferReader.readLine()) != null ) {
            i = lineTxt.indexOf("\t");
            tableName = lineTxt.substring(0,i);
            primaryKeyColumn = lineTxt.substring(i+1,lineTxt.length());
            if (primaryKeyMap.get(tableName) == null){
                primaryKeyMap.put(tableName,new ArrayList<String>());
            }
            primaryKeyMap.get(tableName).add(primaryKeyColumn);
        }
        primaryKeyStreamReader.close();


        //遍历所有输入的文件
        for (File inputFile : inputFilePath.listFiles()){
            //读取列名文档
            ArrayList<String> columnList = new ArrayList<>();
            InputStreamReader columnNameStreamReader = new InputStreamReader(new FileInputStream(inputFile), "UTF-8");
            BufferedReader columnNameBufferReader = new BufferedReader(columnNameStreamReader);
            while ((lineTxt = columnNameBufferReader.readLine()) != null ) {
                if(lineTxt!="\n"){
                    columnList.add(lineTxt);
                }
            }
            columnNameStreamReader.close();


            //取出当前文件用到的表和主键
            String inputFileName = inputFile.getName();
//            tableName = inputFileName.substring(10, inputFileName.length()-3).toUpperCase();
            tableName = inputFileName.substring(5, inputFileName.length()-3).toUpperCase();
            ArrayList<String> primaryKeyList = primaryKeyMap.get(tableName);





            HashMap<String,String> replaceMap = new HashMap<>();
            replaceMap.put("表名",tableName);
//            replaceMap.put("主键list",columnOperate(primaryKeyList,"主键list"));
//            replaceMap.put("主键list-a",columnOperate(primaryKeyList,"主键list-a"));
//            replaceMap.put("主键list-temp",columnOperate(primaryKeyList,"主键list-temp"));
//            replaceMap.put("主键1",columnOperate(primaryKeyList,"主键1"));
//            replaceMap.put("主键list a=keylist",columnOperate(primaryKeyList,"主键list a=keylist"));
//            replaceMap.put("主键list tmp=tmp2",columnOperate(primaryKeyList,"主键list tmp=tmp2"));
            replaceMap.put("列名",columnOperate(columnList,"列名"));
//            replaceMap.put("列名-a",columnOperate(columnList,"列名-a"));
//            replaceMap.put("列名-tmp",columnOperate(columnList,"列名-tmp"));
            replaceMap.put("","");


            //替换
            ArrayList<String> stringArrayList = new ArrayList<>(stringArrayListTemp);
            for(i = 0;i < stringArrayList.size();i++){
                if (stringArrayList.get(i).startsWith("[partdivider]")){
                    k = stringArrayList.get(i).lastIndexOf('#');
                    if (null != replaceMap.get(stringArrayList.get(i).substring(k+1))) {
                        stringArrayList.set(i,replaceMap.get(stringArrayList.get(i).substring(k+1)));
                    }
                }
            }


            //输出
            for (String s : stringArrayList){
                stringBuilder3.append(s);
            }
            stringBuilder3.append("\n\r");
            outputFile =  new File(outputFilePath + "\\dwd_" +tableName+".sh");
            OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outputFile),"UTF-8");
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.append(stringBuilder3);
            bufferedWriter.flush();
            writer.close();
            stringBuilder3.delete(0, stringBuilder3.length());
        }




        return true;
    }

    private static String columnOperate(ArrayList<String> columnList,String operate){
        StringBuilder stringBuilder = new StringBuilder("");
        if (operate.startsWith("主键")){
            if (operate.substring(2).startsWith("list-")){
                String alias = operate.substring(7);
                for (String string : columnList){
                    stringBuilder.append(alias).append(".").append(string).append(",").append("\n");
                }
            }
            if (operate.substring(2).startsWith("list ")){
                String alias1 = operate.substring(7).substring(0,operate.substring(7).indexOf("="));
                String alias2 = operate.substring(7).substring(operate.substring(7).indexOf("=")+1,operate.substring(7).length());
                for (int i = 0;i < columnList.size();i++){
                    String string = columnList.get(i);
                    if (i != 0){
                        stringBuilder.append("and ");
                    }
                    stringBuilder.append(alias1).append(".").append(string).append("=").append(alias2).append(".").append(string).append("\n");
                }
            }
            if (operate.substring(2).equals("list")){
                for (int i = 0;i < columnList.size();i++){
                    String string = columnList.get(i);
                    if (i != 0){
                        stringBuilder.append(",");
                    }
                    stringBuilder.append(string);
                }
            }
            if (operate.substring(2).equals("1")){
                stringBuilder.append(columnList.get(0));
            }
        }
        if (operate.startsWith("列名-")){
            String alias = operate.substring(3);
            for (String string : columnList){
                stringBuilder.append(alias).append(".").append(string).append(",").append("\n");
            }
        }
        if (operate.equals("列名")){
            for (int i = 0;i < columnList.size();i++){
                String string = columnList.get(i);
                if (string.isEmpty()){
                    continue;
                }
                if (i != 0){
                    stringBuilder.append(",");
                }
                stringBuilder.append(string);
            }
        }
        return stringBuilder.toString();
    }
}
