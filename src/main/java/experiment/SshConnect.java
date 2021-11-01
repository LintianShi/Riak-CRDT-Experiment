package experiment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.ArrayList;

import com.jcraft.jsch.*;



public class SshConnect {
    private Session session = null;

    public SshConnect(String host, String passwd) throws JSchException {
        JSch jSch = new JSch();
        session = jSch.getSession("shilintian", host, 22);
        session.setPassword(passwd);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect(3000);
    }

    public List<String> remoteExecute(String command) throws JSchException {
        List<String> resultLines = new ArrayList<>();
        ChannelExec channel = null;
        try{
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            InputStream input = channel.getInputStream();
            channel.connect(3000);
            try {
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(input));
                String inputLine = null;
                while((inputLine = inputReader.readLine()) != null) {
                    resultLines.add(inputLine);
                }
            } finally {
                if (input != null) {
                    try {
                        input.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (channel != null) {
                try {
                    channel.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return resultLines;
    }

    public void close() throws JSchException {
        session.disconnect();
    }
}