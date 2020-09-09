import java.io.*;
import java.util.ArrayList;

/**
 * 本工具用于 利用建表语句生成增量抽数all脚本
 * 使用方法：
 * 1.先把建表语句放在create文件夹里，然后用正则表达式 按 顺 序 进行替换，保证文件内容就是所有的列名
 * (1)   ^(?!.*?(string|decimal|timestamp)).*$    换成  空
 * (2)   [\t ]+(string|decimal|timestamp).*',?    换成  , \
 * (3)   ^[\t| ]*\n  换成  空
 * 2.确保create文件夹中所有文件的名称都是包含表名的，格式要一致
 * 3.把用于拼接的模板文件放在template文件夹中
 * 4.运行后生成的文件在create2文件夹中
 */
public class realdata_all_lis2_generator {
    public static void main(String[] args) throws IOException {
        File inputFile = new File("C:\\Users\\PiaoKing\\Desktop\\create");
        File outputFile = null;
        File templateFile = new File("C:\\Users\\PiaoKing\\Desktop\\template");
        File file = null;
        String fileName = null;
        String tableName = null;
        String lineTxt = null;
        ArrayList<String> strings = new ArrayList<>();
        StringBuilder sb = new StringBuilder("");
        StringBuilder sbtemp1 = new StringBuilder("");
        StringBuilder sbtemp2 = new StringBuilder("");
        int j = 0;
//        System.out.println(outputFile.getAbsoluteFile());

        File[] tempFileList = templateFile.listFiles();
        for(int i = 0;i < tempFileList.length;i++) {
            j=0;
            templateFile = tempFileList[i];
            InputStreamReader read0 = new InputStreamReader(new FileInputStream(templateFile), "UTF-8");//考虑到编码格式
            BufferedReader bufferedReader0 = new BufferedReader(read0);
            while ( (j++==-1)||(lineTxt = bufferedReader0.readLine()) != null ) {
                if(j==1){
                    sbtemp1.append(lineTxt);
                }else{
                    sbtemp1.append("\n"+lineTxt);
                }
//                System.out.println(lineTxt);
            }
            read0.close();
            strings.add(sbtemp1.toString());
            sbtemp1.delete(0, sbtemp1.length());
        }
//        String temp = sb.toString();
//        String temp2 = temp;

        File[] fileList = inputFile.listFiles();
        for(int i = 0;i < fileList.length;i++){
//            temp2 = temp;

            file = fileList[i];
            fileName = file.getName();
            tableName = fileName.substring(10, fileName.length()-3).toUpperCase();
            System.out.println(tableName);

//            temp2 = temp2.replaceAll("njytablename",tableName);

            InputStreamReader read = new InputStreamReader(new FileInputStream(file),"UTF-8");//考虑到编码格式
            BufferedReader bufferedReader = new BufferedReader(read);
            while((lineTxt = bufferedReader.readLine()) != null){
                sbtemp1.append(lineTxt+"\n");
                sbtemp2.append((lineTxt+"\n").replaceAll("\\\\",""));
//                System.out.println(lineTxt);
            }
            read.close();

//            temp2 = temp2.replaceAll("njycolumnname1",sbtemp1.toString());
//            temp2 = temp2.replaceAll("njycolumnname2",sbtemp2.toString());

            sb.append(strings.get(0));
            sb.append(tableName);
            sb.append(strings.get(1));
            sb.append(tableName);
            sb.append(strings.get(2));
            sb.append(tableName);
            sb.append(strings.get(3));
            sb.append(sbtemp1);
            sb.append(strings.get(4));
            sb.append(sbtemp2);
            sb.append(strings.get(5));


            sb.append("\n\r");
            outputFile =  new File("C:\\Users\\PiaoKing\\Desktop\\create2\\sq_O_"+tableName+"_dt_history_all.sh");
            OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outputFile),"UTF-8");
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.append(sb);
            bufferedWriter.flush();
            writer.close();
            sb.delete(0, sb.length());
            sbtemp1.delete(0, sbtemp1.length());
            sbtemp2.delete(0, sbtemp2.length());
        }

    }
}
