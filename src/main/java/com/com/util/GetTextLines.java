package com.com.util;

import java.io.BufferedReader;
import java.io.FileReader;


class GetTextLines {
    int getTextLines(String path) {
        int x = -1;
        try {
            FileReader fr = new FileReader(path);
            BufferedReader br = new BufferedReader(fr);
            while (br.readLine() != null) {
                x++;   //
            }
            return x;

        } catch (Exception e) {
            e.printStackTrace();
            return x;
        }
    }
}
