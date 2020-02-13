package com.talend.cli;


import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.talend.tmc.dom.Executable;
import com.talend.tmc.dom.Execution;
import com.talend.tmc.dom.ExecutionRequest;
import com.talend.tmc.dom.ExecutionResponse;
import com.talend.tmc.services.TalendBearerAuth;
import com.talend.tmc.services.TalendCloudRegion;
import com.talend.tmc.services.TalendCredentials;
import com.talend.tmc.services.TalendRestException;
import com.talend.tmc.services.executables.ExecutableService;
import com.talend.tmc.services.executions.ExecutionService;
import org.apache.cxf.jaxrs.ext.search.client.SearchConditionBuilder;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;


public class tcli {
    private static final Logger logger = Logger.getLogger(tcli.class);
    private static final int _EXITGOOD = 0;
    private static final int _EXITBAD = 1;
    private static ObjectMapper mapper;

    public static void main(String[] args) {

        logger.debug("Passing CLI arguement in to be parsed:: " + Arrays.toString(args));
        Cli.parse(args);
        logger.debug("CLI Arguments parsed successfully");
        logger.info("Getting sorted list of HVR Manifest Files from " + Cli.getCliValue("hm"));

        String token = null;
        if (Cli.hasCliValue("t"))
            token = Cli.getCliValue("t");
        else if (Cli.hasCliValue("te"))
            token = System.getenv(Cli.getCliValue("te"));

        if (token == null) {
            printErrorMessage("Token not found. Please check command and try again.");
        }

        TalendCredentials credentials = new TalendBearerAuth(token);
        try {
            ExecutionService executionService = ExecutionService.instance(credentials, TalendCloudRegion.valueOf(Cli.getCliValue("r")));

            ExecutableService executableService = ExecutableService.instance(credentials, TalendCloudRegion.valueOf(Cli.getCliValue("r")));
            SearchConditionBuilder fiql = SearchConditionBuilder.instance("fiql");

            String envName = Cli.hasCliValue("e") ? Cli.getCliValue("e") : "default";
            String query = fiql.is("name").equalTo(Cli.getCliValue("j")).and().is("workspace.environment.name").equalTo(envName).query();

            Executable[] executables = executableService.getByQuery(query);

            if (executables.length > 1)
                printErrorMessage("More than 1 Job returned with that name!");

            Hashtable<String, String> parameters = null;


            ExecutionRequest executionRequest = new ExecutionRequest();
            executionRequest.setExecutable(executables[0].getExecutable());
            if (Cli.hasCliValue("cv")) {
                String[] pairs = Cli.getCliValue("cv").split(";");
                parameters = new Hashtable<>();
                for (String pair : pairs) {
                    String[] nv = pair.split("=");
                    parameters.put(nv[0], nv[1]);
                }

                executionRequest.setParameters(parameters);
            }

            ExecutionResponse executionResponse = executionService.post(executionRequest);
            printMessage("Talend Job Started: " + executionResponse.getExecutionId());

            if (Cli.hasCliValue("w")) {
                while (true) {

                    Execution execution = executionService.get(executionResponse.getExecutionId());
                    if (execution.getFinishTimestamp() != null) {
                        if (!execution.getExecutionStatus().equals("EXECUTION_SUCCESS"))
                            printErrorMessage("Job Completed in non Successful State :" + execution.toString());

                        break;
                    } else {
                        Thread.sleep(5000);
                    }
                }

                System.exit(_EXITGOOD);
            }
        } catch(TalendRestException | IOException | InterruptedException ex){
            printErrorMessage(ex.getMessage());
        }
    }

    private static void printMessage(String message)
    {
        System.out.println(message);
    }

    private static void printErrorMessage(String message) {
        System.err.println(message);
        System.exit(_EXITBAD);
    }

}