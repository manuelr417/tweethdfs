package edu.uprm.ths.tweethdfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        System.out.println( "Hello World!" );

        Logger logger = LogManager.getRootLogger();
        logger.trace("Esto se jodio aqui!");
    }
}
