/*
 * Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
 *
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.basho.yokozuna.monitor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fire off a shell script to kill Solr once spawning Erlang has died
 * 
 * @author Brett Hazen
 */
public class Monitor {
    protected static Logger log = LoggerFactory.getLogger(Monitor.class);
    
    /**
     * Launches monitoring bash script
     * @param monitorScript name of script to run
     * @param sleepSeconds interval at which to run PPID script
     */
    public static void monitor(final String monitorScript, final String sleepSeconds) {
        int sleepInterval = Integer.parseInt(sleepSeconds);
        TimerTask readyForSuicide = new TimerTask() {
           @Override
           public void run() {
               try {
                   String[] cmd = {monitorScript};
                   Process p = Runtime.getRuntime().exec(cmd);
                   BufferedReader buf = new BufferedReader(new InputStreamReader(p.getInputStream()));
                   p.waitFor();
                   // Read single output line to determine PPID
                   String line = buf.readLine();
                   int exitValue = p.exitValue();
                   if (exitValue != 0) {
                       System.err.println("Problem running Solr monitoring script");
                   }
                   // Die if our parent is no longer Erlang
                   int ppid = Integer.parseInt(line);
                   if (ppid == 1) {
                       System.err.println("Shutting down Solr after Yokozuna has finished");
                       log.warn("Shutting down Solr after Yokozuna has finished");
                       System.exit(0);
                   }
               } catch(IOException e) {
                   System.err.println(e.getMessage());
                   log.error(e.getMessage());
               } catch(InterruptedException e) {
                   System.err.println(e.getMessage());
                   log.error(e.getMessage());
               }
           }
       };
       Timer timer = new Timer();

       // scheduling the task at fixed rate
       timer.scheduleAtFixedRate(readyForSuicide,new Date(), sleepInterval*1000);      
    }

   
    /**
     * Main for testing
     * @param monitorScript name of script to run
     * @param sleepSeconds interval at which to run PPID script
     */
    public static void main(String[] args) {
        monitor(args[0], args[1]);
    }
}
