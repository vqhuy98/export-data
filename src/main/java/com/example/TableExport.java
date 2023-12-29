package com.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.tika.Tika;

/**
 * Hello world!
 *
 */
public class TableExport
{
    private static final String SCHEMA_NAME = "testdatabase";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/" + SCHEMA_NAME;
    private static final String TABLE_NAME = "information";
    private static final String USER = "root";
    private static final String PASSWORD = "password";
    private static final String IMAGE_PATH = "src/main/resources/image1.jpg";

    public static void main( String[] args )
    {

        System.out.println( "Hello World!" );


//        imageBlobInsert();

//        if (args.length != 5) {
//            System.out.println("Usage: java TableExport <dbUrl> <username> <password> <exportPath> <schemaName>");
//            return;
//        }
//
//        String dbUrl = args[0];
//        String username = args[1];
//        String password = args[2];
//        String exportPath = args[3];
//        String schemaName = args[4];


        String dbUrl = DB_URL;
        String username = USER;
        String password = PASSWORD;
        String exportPath = "/home/vqhuy01/idea/my-console-app/src/main/resources";
        String schemaName = SCHEMA_NAME;

        try (Connection connection = DriverManager.getConnection(dbUrl, username, password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(connection.getCatalog(), schemaName, "%", new String[]{"TABLE"});

            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                File tableFolder = new File(exportPath + File.separator + tableName);
                tableFolder.mkdirs();

                //export excel file
                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName)) {

                    ResultSetMetaData rsMetaData = resultSet.getMetaData();
                    int columnCount = rsMetaData.getColumnCount();
                    createExcelFile(resultSet, rsMetaData, tableName,columnCount, tableFolder + File.separator + tableName+".xlsx");

                }
                //export blob column
                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName)) {

                    ResultSetMetaData rsMetaData = resultSet.getMetaData();
                    int columnCount = rsMetaData.getColumnCount();
                    exportBlobColumn(resultSet, rsMetaData ,columnCount, tableFolder);

                }
            }
            System.out.println("Export completed!");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void imageBlobInsert() {
        File imageFile = new File(IMAGE_PATH);

        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
            // Create a PreparedStatement to insert data
            String sql = "UPDATE information " +
                "SET image = (?)" +
                "WHERE id = 5;";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                FileInputStream inputStream = new FileInputStream(imageFile);
                // Set the BLOB parameter
                statement.setBinaryStream(1, inputStream, (int) imageFile.length());
                // Execute the query
                int rowsAffected = statement.executeUpdate();
                if (rowsAffected > 0) {
                    System.out.println("Image inserted successfully!");
                } else {
                    System.out.println("Failed to insert image.jpg!");
                }
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void createExcelFile( ResultSet resultSet, ResultSetMetaData rsMetaData,String tableName, int columnCount, String tableFolder ) throws SQLException {
        Workbook workbook = new XSSFWorkbook();
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
                if(!rsMetaData.getColumnTypeName(i).equalsIgnoreCase("LONGBLOB")) {
                    cell.setCellValue(resultSet.getString(i));
                }else {
                    cell.setCellValue("<LONGBLOB>");
                }
            }
        }
        // Write workbook to file
        try (FileOutputStream fileOut = new FileOutputStream(tableFolder)) {
            workbook.write(fileOut);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      System.out.printf("Excel file exported successfully for table %s !%n",tableName);

    }

    private static void exportBlobColumn(ResultSet resultSet, ResultSetMetaData rsMetaData, int columnCount, File tableFolder) throws Exception {
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsMetaData.getColumnName(i);
                if (rsMetaData.getColumnTypeName(i).equalsIgnoreCase("LONGBLOB")) {
                    Blob blob = resultSet.getBlob(columnName);
                    if (blob != null) {
                        File recordFolder = new File(tableFolder + File.separator + resultSet.getString(1));
                        recordFolder.mkdirs();

                        try (InputStream inputStream = blob.getBinaryStream()) {
                            // find the file extension
                            Tika tika = new Tika();
                            String detectedMimeType = tika.detect(inputStream);
                            String fileExtension = detectedMimeType.equals("application/octet-stream")
                                ? "" // If unknown, keep empty extension or set a default extension
                                : "." + detectedMimeType.split("/")[1]; // Use detected MIME type to get extension

                            File file = new File(recordFolder + File.separator + columnName + fileExtension);
                            Files.copy(inputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
                        }
                    }
                }
            }
        }
    }
}
