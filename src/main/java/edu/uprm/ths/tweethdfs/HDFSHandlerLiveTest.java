package edu.uprm.ths.tweethdfs;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.time.LocalDate;

/**
 * Created by manuel on 2/19/17.
 */
public class HDFSHandlerLiveTest {
    public static void main(String[] args) throws Exception{
        System.out.println("Puneta!!!!!!");
        Logger logger = LogManager.getRootLogger();
        logger.trace("Starting the HDFS Handler Live Test");

        HDFSHandler handler = HDFSHandler.init("hdfs://masternode.ece.uprm.edu:9000","/home/manuel/test");
        logger.trace("Open the handler");
        handler.open();
        logger.trace("Writing 100 strings to the file.");
        for (int i=0; i < 100; ++i){
            if ((i % 10) == 0){
                logger.trace(".");
            }
            handler.writeUTF("This is string number : " + i);
        }
        logger.trace("Closing the handler");
        handler.close();
        logger.trace("Done!");
    }
}
