package com.market.efx.service;

import com.market.efx.model.MarketPrice;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import jdk.jfr.Timestamp;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Component
public class CSVUtil {

    public void CSVWriter(MarketPrice marketPrice) throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter("output.csv", true));

        try {
            writer.writeNext(new String[]{marketPrice.getUniqueId(), marketPrice.getInstrumentName()
                    , marketPrice.getBid().toString(), marketPrice.getAsk().toString(), marketPrice.getTimestamp()});

            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}