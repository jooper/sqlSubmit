package org.apache.flink;

import org.apache.flink.table.factories.TableFactory;

import java.util.ServiceLoader;

public class test {
    public static void main(String[] args) {
        ServiceLoader<TableFactory> uploadCDN = ServiceLoader.load(TableFactory.class);

        for (TableFactory u : uploadCDN) {
            System.out.println(u.getClass());
        }
    }
}
