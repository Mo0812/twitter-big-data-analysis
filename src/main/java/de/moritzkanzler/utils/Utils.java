package de.moritzkanzler.utils;

import de.moritzkanzler.exception.GeoCoordinatesNotFoundException;
import de.moritzkanzler.exception.ParameterNotFoundException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * This utils class implement different methods which are used in the whole project
 */
public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    private static Map<String, GeoCoordinates> langMapping = null;

    /**
     * Escape strings for further csv exports
     * @param s
     * @return
     */
    public static String escape(String s) {
        String escapedString = StringEscapeUtils.escapeCsv(s);
        escapedString = escapedString.replace("\"", "\\\"");
        return "\'" + escapedString + "\'";
    }

    /**
     * Rounds decimal values to a maximum of decimalPlace steps after the decimal point
     * @param d
     * @param decimalPlace
     * @return
     */
    public static Double roundDouble(Double d, Integer decimalPlace) {
        BigDecimal bd = new BigDecimal(d);
        bd = bd.setScale(decimalPlace, RoundingMode.HALF_EVEN);
        return bd.doubleValue();
    }

    /**
     * This function tries to map a langcode to the coordinates of the capital city of such a country. For this mapping the resources/country-capital JSON file is used
     * @param langCode
     * @return
     * @throws ParameterNotFoundException
     * @throws GeoCoordinatesNotFoundException
     */
    public static GeoCoordinates langCodeToCoords(String langCode) throws ParameterNotFoundException, GeoCoordinatesNotFoundException {
        if(langMapping == null) {
            try {
                ParameterTool parameterTool = ParameterTool.fromPropertiesFile("src/main/resources/general.properties");
                if(parameterTool.has("lang.gps.json")) {
                    langMapping = readJSON(parameterTool.get("lang.gps.json"));
                } else {
                    throw new ParameterNotFoundException("Parameter for GPS JSON file not found");
                }
            } catch(Exception e) {
                throw new ParameterNotFoundException("File with parameters not found");
            }
        }
        GeoCoordinates geoCoordinates = langMapping.get(escapeLangCode(langCode));

        if(geoCoordinates == null) {
            throw new GeoCoordinatesNotFoundException();
        } else {
            return geoCoordinates;
        }
    }

    /**
     * Escapes a langcode if this is not only represented through two letters but four (important for the difference of US and UK i.e)
     * @param langCode
     * @return
     */
    private static String escapeLangCode(String langCode) {
        langCode = langCode.trim();
        String[] split = langCode.split("-");
        if(split.length > 1) {
            return split[1].toLowerCase();
        } else {
            return translateMissingLangCode(split[0]).toLowerCase();
        }
    }

    /**
     * As seen in the data the twitter data source maps some langcodes different two countries as the used capital city list, here happens a tranformation
     * @param langCode
     * @return
     */
    private static String translateMissingLangCode(String langCode) {
        switch(langCode) {
            case "en":
                return "us";
            case "ja":
                return "jp";
            case "el":
                return "gr";
            case "da":
                return "dk";
            case "uk":
                return "ua";
            case "fa":
                return "ir";
            case "hi":
                return "in";
            default:
                return langCode;

        }
    }

    /**
     * Method to process the capital city json file
     * @param filename
     * @return
     */
    private static Map<String, GeoCoordinates> readJSON(String filename) {
        try {
            JSONParser jsonParser = new JSONParser();
            JSONArray all = (JSONArray) jsonParser.parse(new FileReader(filename));

            Map<String, GeoCoordinates> returnMap = new HashMap<>();

            for (Object obj : all) {
                JSONObject capital = (JSONObject) obj;

                String lat = (String) capital.get("CapitalLatitude");
                String lng = (String) capital.get("CapitalLongitude");
                String langCode = (String) capital.get("CountryCode");
                if(langCode != null && lat != null && lng != null) {
                    returnMap.put(langCode.trim().toLowerCase(), new GeoCoordinates(lat, lng));
                }
            }

            return returnMap;
        } catch (Exception e) {
            logger.warn(e.getLocalizedMessage());
            return new HashMap<>();
        }
    }

    @Deprecated
    public static void removeLastLine(String path) {
        try {
            BufferedReader in = Files.newBufferedReader(Paths.get(path));
            BufferedWriter out = Files.newBufferedWriter(Paths.get(path + ".new"));
            in.lines().forEach(line-> {
                if(line.split(",").length == 4) {
                    try {
                        out.write(line);
                        out.newLine();
                    } catch (Exception e) {
                        logger.error(e.getLocalizedMessage());
                    }
                }
            });
            in.close();
            out.close();

            Files.deleteIfExists(Paths.get(path));
            Files.move(Paths.get(path + ".new"), Paths.get(path));
        } catch(Exception e) {
            logger.error(e.getLocalizedMessage());
        }
    }
}
