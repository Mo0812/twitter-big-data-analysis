package de.moritzkanzler.helper;

import de.moritzkanzler.exception.ParameterNotFoundException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.net.UnknownHostException;

/**
 * This class is a wrapper for the ParameterTool from the apache flink project. It enables the option to get parameters especially for the different use cases of the metrics.
 * It also enables the function to search for requested parameters not only in the job specific property file but also in the general property file of the project
 */
public class JobParameters {
    private static JobParameters instance = null;
    private static Logger logger = LoggerFactory.getLogger(JobParameters.class);

    private ParameterTool parameterTool;
    private ParameterTool generalParameterTool;

    /**
     * Singleton functionality
     * @param file
     * @return
     */
    public static JobParameters getInstance(String file) {
        if(instance == null) {
            instance = new JobParameters(file);
        }
        return instance;
    }

    /**
     * Constructor
     * @param file
     */
    protected JobParameters(String file) {
        try {
            this.parameterTool = ParameterTool.fromPropertiesFile(file);
            this.generalParameterTool = ParameterTool.fromPropertiesFile("src/main/resources/general.properties");
        } catch (Exception e) {
            logger.error("Searched parameter file not found");
        }
    }

    /**
     * Get property with JobType prefix (STREAM, BATCH, PREDICTION, GENERAL). If the looked up property do not exist in the job property file it looks it also up in the general property file
     * @param type
     * @param name
     * @return
     * @throws ParameterNotFoundException
     */
    public String getProperty(JobType type, String name) throws ParameterNotFoundException {
        String propertyName = getJobType(type) + "." + name;
        if(!parameterTool.has(propertyName)) {
            if(!generalParameterTool.has(name)) {
                throw new ParameterNotFoundException("Parameter: " + propertyName + " not found or not defined");
            } else {
                return generalParameterTool.get(name);
            }
        } else {
            return parameterTool.get(propertyName);
        }
    }

    public String getProperty(JobType type, String name, String defaultVal) {
        try {
            return this.getProperty(type, name);
        } catch (ParameterNotFoundException e) {
            logger.warn(e.getLocalizedMessage());
            return defaultVal;
        }
    }

    public boolean getProperty(JobType type, String name, boolean defaultVal) {
        try {
            return Boolean.parseBoolean(this.getProperty(type, name));
        } catch (ParameterNotFoundException e) {
            logger.warn(e.getLocalizedMessage());
            return defaultVal;
        }
    }

    public Integer getProperty(JobType type, String name, Integer defaultVal) {
        try {
            return Integer.parseInt(this.getProperty(type, name));
        } catch (ParameterNotFoundException e) {
            logger.warn(e.getLocalizedMessage());
            return defaultVal;
        }
    }

    /**
     * Mapper of JobType enum to string representation in the property files
     * @param jobType
     * @return
     */
    private String getJobType(JobType jobType) {
        switch(jobType) {
            case STREAM:
                return "stream";
            case BATCH:
                return "batch";
            case PREDICTION:
                return "prediction";
            default:
                return "general";
        }
    }
}
