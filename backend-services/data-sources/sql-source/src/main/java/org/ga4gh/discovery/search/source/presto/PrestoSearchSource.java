/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.source.presto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.result.ResultRow;
import org.ga4gh.discovery.search.result.SearchResult;
import org.ga4gh.discovery.search.source.SearchSource;

/**
 *
 * @author mfiume
 */
public class PrestoSearchSource implements SearchSource {

    private final String driverClassName;
    private final String url;
    private final String username;
    private final String password;
    private List<Field> fields;

    public PrestoSearchSource(String url, String username, String password, String driverClassName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
    }
    
    private List<Field> updateFields() {
        try {
            List<Field> fields = new ArrayList<Field>();
            
            String tableName = "drs.org_ga4gh_drs.objects";
            
            //Class.forName(driverClassName);
            Connection connection = DriverManager.getConnection(
                    url,
                    username,
                    password);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("show columns from " + tableName);  

            while (rs.next()) {
                String fieldName = rs.getString(1);
                Field.Type fieldType = prestoToPrimativeType(rs.getString(2));
                fields.add(new Field(
                        tableName + "." + fieldName,
                        fieldName,
                        fieldType,
                        operatorsForType(fieldType), 
                        new String[0],
                        tableName
                ));
            }
            
            connection.close();
            return fields;
        } catch (Exception ex) {
            Logger.getLogger(PrestoSearchSource.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
    
    @Override
    public List<Field> getFields() {
        if (fields == null) { // todo: should also check if the table schema has been altered since last call to this method
            fields = updateFields();
        }
        return fields;
    }

    @Override
    public SearchResult search(SearchQuery query) {
        List<ResultRow> results = new ArrayList<>();
        SearchResult searchResult = new SearchResult(getFields(),results);
        return searchResult;
    }

    private Field.Type sqlToPrimativeType(String sqlType) {
        if (sqlType.startsWith("int")) {
            return Field.Type.NUMBER;
        } else if (sqlType.startsWith("varchar")) {
            return Field.Type.STRING;
        }
        throw new RuntimeException("Unknown mapping for SQL field type " + sqlType);
    }
    
    private String[] operatorsForType(Field.Type type) {
        switch (type) {
            case NUMBER:
                return new String[] { "=", "!=", "<", "<=", ">", ">=" };
            case STRING:
                return new String[] { "=", "!=", "contains", "like" };
            case DATE:
                return new String[] { "=", "!=", "<", "<=", ">", ">=" };
            case STRING_ARRAY:
                return new String[] { "all", "in", "none" };
            case BOOLEAN:
                return new String[] {};
            case JSON:
                return new String[] { "=", "!=" };
            default :
                return new String[0];
        }
    }

    private Field.Type prestoToPrimativeType(String prestoType) {
        if (prestoType.equals("integer") || prestoType.equals("double") || prestoType.equals("bigint")) {
            return Field.Type.NUMBER;
        } else if (prestoType.equals("timestamp")){
            return Field.Type.DATE;
        } else if (prestoType.startsWith("boolean")) {
            return Field.Type.BOOLEAN;
        } else if (prestoType.startsWith("varchar")) {
            return Field.Type.STRING;
        } else if (prestoType.startsWith("array(varchar)")) {
            return Field.Type.STRING_ARRAY;
        } else if (prestoType.startsWith("array(row")) {
            return Field.Type.JSON;
        }
        throw new RuntimeException("Unknown mapping for Presto field type " + prestoType);
    }
    
    
    
}
