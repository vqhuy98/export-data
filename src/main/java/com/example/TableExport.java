package com.example;


import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.tika.Tika;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.Arrays;
import java.util.List;


public class TableExport {
//    private static final String SCHEMA_NAME = "testdatabase";
//    private static final String DB_URL = "jdbc:mysql://localhost:3306/" + SCHEMA_NAME;
//    private static final String USER = "root";
//    private static final String PASSWORD = "password";
//    private static final String EXPORT_PATH = "C:\\exportfile";


    public static void main(String[] args) {

        if (args.length < 5) {
            System.out.println("Please provide arguments in the following order: DB_URL SCHEMA_NAME USER PASSWORD EXPORT_PATH");
            return;
        }

        String DB_URL = args[0];
        String SCHEMA_NAME = args[1];
        String USER = args[2];
        String PASSWORD = args[3];
        String EXPORT_PATH = args[4]; // Split comma-separated table names

        List<String> tableNames = Arrays.asList("ext_change_international_sponsor"
                , "ext_change_los", "ext_merge_distributorship"
                , "ext_seperate_distributorship", "ext_placeholder", "ext_transfer_pv_bv_checklist", "ext_sod_priority");
        try (Connection connection = DriverManager.getConnection(DB_URL+SCHEMA_NAME, USER, PASSWORD)) {
            System.out.print("Connect successfully \n");
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(connection.getCatalog(), SCHEMA_NAME, "%", new String[]{"TABLE"});

            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                if (tableNames.contains(tableName)) {
                    System.out.println("===========================================================");
                    System.out.printf("Export for table: %s \n", tableName);
                    File tableFolder = new File(EXPORT_PATH + File.separator + tableName);
                    tableFolder.mkdirs();
                    System.out.print("start query all data \n");
                    try (Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                         ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName)) {
                        System.out.printf("select all for table: %s successfully \n", tableName);
                        ResultSetMetaData rsMetaData = resultSet.getMetaData();
                        int columnCount = rsMetaData.getColumnCount();

                        //export excel file
                        createExcelFile(resultSet, rsMetaData, tableName, columnCount, tableFolder + File.separator + tableName + ".xlsx");

                        resultSet.beforeFirst();

                        //export blob column
                        exportBlobColumn(connection, resultSet, rsMetaData, columnCount, tableFolder, SCHEMA_NAME);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                }
            }
            System.out.println("Export completed!");
            System.out.println("===========================================================");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static void createExcelFile(ResultSet resultSet, ResultSetMetaData rsMetaData, String tableName, int columnCount,
                                        String tableFolder) throws SQLException, IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            System.out.printf("start create excel file for table %s with %s column \n", tableName, columnCount);
            Sheet sheet = workbook.createSheet(tableName);

            // Create header row
            Row headerRow = sheet.createRow(0);
            for (int i = 1; i <= columnCount; i++) {
                Cell cell = headerRow.createCell(i - 1);
                cell.setCellValue(rsMetaData.getColumnName(i));
            }
            // Fill data rows
            int rowNum = 1;
            while (resultSet.next()) {
                Row row = sheet.createRow(rowNum++);
                for (int i = 1; i <= columnCount; i++) {
                    sheet.autoSizeColumn(i);
                    Cell cell = row.createCell(i - 1);
                    if (!rsMetaData.getColumnTypeName(i).contains("BLOB")) {
                        cell.setCellValue(resultSet.getString(i));
                    } else {
                        cell.setCellValue("<BLOB>");
                    }
                }
            }
            System.out.printf("Start write data to file %s \n", tableFolder);
            // Write workbook to file
            try (FileOutputStream fileOut = new FileOutputStream(tableFolder)) {
                workbook.write(fileOut);
                System.out.printf("write data to file %s successfully \n", tableFolder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.printf("End write data to file %s \n", tableFolder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.printf("stop Excel file exported for table %s ! \n", tableName);

    }

    private static void exportBlobColumn(Connection connection, ResultSet resultSet, ResultSetMetaData rsMetaData, int columnCount,
                                         File tableFolder, String SCHEMA_NAME) throws Exception {
        System.out.println("Start export Blob Column by PROC_INST_ID_");

        int proInstIdIndex = findColumnNameIndex("PROC_INS_ID_", rsMetaData, columnCount);

        if (proInstIdIndex == -1) {
            System.out.println("PROC_INST_ID_ column not found.");
            return;
        }
        while (resultSet.next()) {
            String procInstId = resultSet.getString(proInstIdIndex);

            // Create the query based on PROC_INST_ID_
            String query = "SELECT b.NAME_, a.BYTES_" +
                    " FROM " + SCHEMA_NAME + ".act_ge_bytearray a " +
                    "JOIN " + SCHEMA_NAME + ".act_hi_attachment b ON a.ID_ = b.CONTENT_ID_ " +
                    "WHERE b.PROC_INST_ID_ = '" + procInstId + "'";

            try (Statement blobStatement = connection.createStatement();
                 ResultSet blobResultSet = blobStatement.executeQuery(query)) {

                while (blobResultSet.next()) {
                    String blobName = blobResultSet.getString("NAME_");
                    Blob blob = blobResultSet.getBlob("BYTES_");
                    if (blob != null) {
                        File recordFolder = new File(tableFolder + File.separator + resultSet.getString(1));
                        recordFolder.mkdirs();

                        try (InputStream inputStream = blob.getBinaryStream()) {
                            // find the file extension
                            Tika tika = new Tika();
                            String detectedMimeType = tika.detect(inputStream);
//                            String fileExtension = detectedMimeType.equals("application/octet-stream")
//                                    ? "" // If unknown, keep empty extension or set a default extension
//                                    : "." + detectedMimeType.split("/")[1]; // Use detected MIME type to get extension

                            File file = new File(recordFolder + File.separator + blobName);
                            Files.copy(inputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
                        }
                    }
                }
            }
        }
        System.out.print("stop export Blob Column \n");
    }

    private static int findColumnNameIndex(String OgColumnName, ResultSetMetaData rsMetaData, int columnCount) throws SQLException {
        String columnName;
        int index = -1;

        // Find the index of column
        for (int i = 1; i <= columnCount; i++) {
            columnName = rsMetaData.getColumnName(i);
            if (columnName.equalsIgnoreCase(OgColumnName)) {
                index = i;
                break;
            }
        }
        return index;
    }
}
