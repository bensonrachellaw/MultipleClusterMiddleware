package com.netty.rpc.common.transferUtils;

import java.io.*;
import java.util.Base64;

public class IOFileUtils {

    public static void sendFile(DataOutputStream out, String AESKey, String FILEPATH) throws IOException {
        try (DataInputStream fileIn = new DataInputStream(new BufferedInputStream(new FileInputStream(FILEPATH)))) {
            // 进度条
            ConsoleProgressBarUtils cpb = new ConsoleProgressBarUtils(50,100, '#');
            File file = new File(FILEPATH);

            System.out.println("FileName:  " + file.getName());
            out.writeUTF(file.getName());
            out.flush();
            System.out.println("FileSize:  " + file.length() / 1024 + " KB");
            out.writeLong(file.length());
            out.flush();

            long transfered = 0;
            int transferedRate = 0;
            while (true) {
                byte[] buf = new byte[4096];
                int read = fileIn.read(buf);
                out.writeInt(read);
                transfered += read;
                if (read == -1)
                    break;
                if ((int) (transfered * 100 / file.length()) > transferedRate) {
                    transferedRate = (int) (transfered * 100 / file.length());
                    cpb.show(transferedRate);
                }

                buf = AES.encrypt(buf, AESKey);

                String str = Base64.getEncoder().encodeToString(buf);
                out.writeUTF(str);
                out.flush();
            }
            System.out.println("Have   Transfered  !");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void receiveFile(DataInputStream in, String Key, String savePath) throws Exception {
        System.out.println("Start  receiving:  ");
        String fileName = in.readUTF();
        long len;
        System.out.println("FileName:  " + fileName);
        len = in.readLong();
        System.out.println("FileSize:  " + len / 1024 + " KB");
        File file = new File(savePath);
        // 保存路径不存在则创建
        if(!file.exists()){
            file.mkdir();
        }
        DataOutputStream fileOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(savePath + fileName)));
        // 进度条
        ConsoleProgressBarUtils cpb = new ConsoleProgressBarUtils(50,100, '#');
        long received = 0;
        int receivedRate = 0;
        while (true) {
            int read = in.readInt();
            received += read;
            if (read == -1) {
                break;
            }
            if ((int) (received * 100 / len) > receivedRate) {
                receivedRate = (int) (received * 100 / len);
                cpb.show(receivedRate);
            }
            String str = in.readUTF();
            byte[] buf = AES.decrypt(Base64.getDecoder().decode(str), Key);
            fileOut.write(buf, 0, read);
        }
        fileOut.close();
        System.out.println("Have Received , Saved as:   " + savePath + fileName);
    }
}
