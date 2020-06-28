package com.github.superzhc.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 项目打包工具
 * 2020年06月28日 superz add
 */
public class ProjectPackageTool
{
    private static final Logger logger = LoggerFactory.getLogger(ProjectPackageTool.class);
    
    public static void doPackage() {
        new ProjectPackageTool().execute();
    }

    private void execute() {
        try {
            // 获取当前项目的路径
            String path = this.getClass().getClassLoader().getResource(".").getPath();
            path = path.replace("/target/classes/", "");

            String mvn = String.format("mvn clean package -Dmaven.test.skip=true -f %s/pom.xml -e", path.substring(1));
            String cmd = String.format("cmd /c %s", mvn);
            logger.info("打包执行命令：[{}]", cmd);
            Process process = Runtime.getRuntime().exec(cmd);

            if (logger.isDebugEnabled()) {
                // // 获取子进程的输出流
                // InputStream in=process.getInputStream();
                // // 获取子进程的错误流
                // InputStream error=process.getErrorStream();
                // SequenceInputStream是一个串联流，能够把两个流结合起来，通过该对象就可以将getInputStream方法和getErrorStream方法获取到的流一起进行查看了，当然也可以单独操作
                SequenceInputStream sis = new SequenceInputStream(process.getInputStream(), process.getErrorStream());
                InputStreamReader inst = new InputStreamReader(sis, "GBK");// 设置编码格式并转换为输入流
                BufferedReader br = new BufferedReader(inst);// 输入流缓冲区
                String res = null;
                while ((res = br.readLine()) != null) {// 循环读取缓冲区中的数据
                    logger.debug(res);
                    // Building jar: D:\superz\FileParsing\SparkLivyExecuteor\target\SparkLivyExecuteor-0.3.0.jar
                    if(res.startsWith("[INFO] Building jar: ")){
                        System.setProperty("livy.job.jars",res.substring(21));
                    }
                }
                br.close();
            }
            process.waitFor();
            process.destroy();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
