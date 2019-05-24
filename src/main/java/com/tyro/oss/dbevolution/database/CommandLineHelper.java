/*
 * Copyright 2019 Tyro Payments Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tyro.oss.dbevolution.database;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CommandLineHelper {

    private static final Log LOG = LogFactory.getLog(CommandLineHelper.class);

    public String getHostname() {
        try {
            return executeCommand(new String[]{"hostname"}).replace("\n", "");
        } catch (CommandExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public String executeCommand(String[] osCommand) throws CommandExecutionException {
        String commandResult;
        try {
            LOG.debug("Executing " + Arrays.toString(osCommand));

            final Process process = Runtime.getRuntime().exec(osCommand);
            StreamReader outputReader = new StreamReader(process.getInputStream());
            StreamReader errorReader = new StreamReader(process.getErrorStream());

            ExecutorService executor = Executors.newCachedThreadPool();

            Future<String> outputReaderFuture = executor.submit(outputReader);
            Future<String> errorReaderFuture = executor.submit(errorReader);

            process.waitFor();
            commandResult = outputReaderFuture.get();
            String stdErrorOutput = errorReaderFuture.get();

            if (process.exitValue() != 0) {
                String errString = "stderr: [" + stdErrorOutput + "]";
                LOG.error(errString);
                throw new CommandExecutionException(errString);
            }
            return commandResult;
        } catch (Exception e) {
            LOG.error("Error executing command: " + Arrays.toString(osCommand), e);
            throw new CommandExecutionException(e);
        }
    }

    public void executeCommand(String[] osCommand, File outputFile, boolean append) throws CommandExecutionException {
        try {
            LOG.debug("Executing " + Arrays.toString(osCommand));

            Process process = Runtime.getRuntime().exec(osCommand);
            StreamPiper outputPiper = new StreamPiper(process.getInputStream(), new FileOutputStream(outputFile, append));
            StreamReader errorReader = new StreamReader(process.getErrorStream());

            ExecutorService executor = Executors.newCachedThreadPool();

            Future<Object> outputPiperFuture = executor.submit(outputPiper);
            Future<String> errorReaderFuture = executor.submit(errorReader);

            process.waitFor();
            outputPiperFuture.get();
            String stdErrorOutput = errorReaderFuture.get();

            if (process.exitValue() != 0) {
                String errString = "stderr: [" + stdErrorOutput + "]";
                LOG.error(errString);
                throw new CommandExecutionException(errString);
            }
        } catch (Exception e) {
            LOG.error("Error executing command", e);
            throw new CommandExecutionException(e);
        }
    }

    private static class StreamReader implements Callable<String> {

        private BufferedReader reader;

        public StreamReader(InputStream inputStream) {
            reader = new BufferedReader(new InputStreamReader(inputStream));
        }

        @Override
        public String call() {
            StringBuilder builder = new StringBuilder();
            try {
                for (String next = reader.readLine(); next != null; next = reader.readLine()) {
                    builder.append(next).append("\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return builder.toString();
        }
    }

    private static class StreamPiper implements Callable<Object> {

        private final InputStream inputStream;
        private final OutputStream outputStream;

        public StreamPiper(InputStream inputStream, OutputStream outputStream) {
            this.inputStream = inputStream;
            this.outputStream = outputStream;
        }

        @Override
        public Object call() throws IOException {
            byte[] bytes = new byte[512];
            int bytesRead;
            while ((bytesRead = inputStream.read(bytes)) != -1) {
                outputStream.write(bytes, 0, bytesRead);
            }
            outputStream.close();
            return null;
        }
    }
}
