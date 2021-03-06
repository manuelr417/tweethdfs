package edu.uprm.ths.tweethdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.local.LocalConfigKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.LoggingStateChangeListener;
import org.apache.hadoop.util.Progressable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.time.LocalDate;

/**
 * Created by manuel on 2/19/17.
 */
public class HDFSHandler {

    private static  HDFSHandler theInstance = null;

    private static HDFSHandler getTheInstance(){
        return theInstance;
    }

    private LocalDate lastDate;
    private String hdfsURI;
    private String filePrefix;
    private HDFSHandlerState currentState;
    private Configuration conf;
    private FSDataOutputStream outputStream;
    private PrintStream printStream;

    private HDFSHandler(String hdfsURI, String filePrefix){
        this.hdfsURI = hdfsURI;
        this.filePrefix = filePrefix;
        this.currentState = HDFSHandlerState.NEW;
        this.conf = new Configuration();
    }

    public static HDFSHandler init(String hdfsURI, String filePrefix) throws IOException {
        if ((hdfsURI == null) || (filePrefix == null)){
            throw new IllegalArgumentException("Null parameter.");
        }
        theInstance = new HDFSHandler(hdfsURI, filePrefix);
        theInstance.open();
        return theInstance;
    }

    private boolean isEqualDate(LocalDate d){
        return (this.lastDate != null) && this.lastDate.equals(d);
    }

    public HDFSHandler open() throws IOException{
        this.setUpHDFSFile();
        this.currentState = HDFSHandlerState.OPEN;
        return this;
    }

    public HDFSHandler close() throws IOException{
        this.printStream.close();
        this.outputStream.close();
        this.currentState = HDFSHandlerState.CLOSED;
        return this;
    }

    public HDFSHandler writeUTF(String str) throws IOException{
        if (str == null){
            throw new IllegalArgumentException("Parameter cannot be null.");
        }
//        Logger logger = LogManager.getRootLogger();
//        LocalDate now = LocalDate.now();
//        logger.trace("LocalDate.now: " + now);
//        logger.trace("Last Dat=e: " + this.lastDate);
//        logger.trace("isEqualDate: " + this.isEqualDate(now));
        if (!this.isEqualDate(LocalDate.now())){
            this.setUpHDFSFile();
        }
//        byte[] buf = str.getBytes();
//        ByteArrayInputStream dataIn = new ByteArrayInputStream(buf);
//        IOUtils.copyBytes(dataIn, this.outputStream, buf.length, false);
//        this.outputStream.flush();
//        //this.outputStream.write(buf, 0, buf.length);
        //this.outputStream.writeUTF(str);
        this.printStream.println(str);
        return this;
    }

    private void setUpHDFSFile() throws IOException{
        if (this.currentState == HDFSHandlerState.CLOSED){
            throw new IllegalStateException("HDFSHandler cannot be closed when running this operation.");
        }
        if (this.currentState == HDFSHandlerState.OPEN){
            this.close();
        }
        LocalDate newDate = LocalDate.now();
        String filePathName = filePrefix + "-" + newDate.toString();
        System.out.println("CABRON FILE: " + filePathName);
        URI fileUri = URI.create(filePathName);
        FileSystem hdfs = FileSystem.get(fileUri, conf);
        this.outputStream =  hdfs.create(new Path(filePathName), new Progressable() {
            public void progress() {
                //System.out.print(".");
            }});
        this.printStream = new PrintStream(this.outputStream);
        this.lastDate = newDate;
        //FileSystem hdfs = FileSystem.get(conf);
        //this.outputStream = hdfs.append(new Path(filePathName));
    }
}
