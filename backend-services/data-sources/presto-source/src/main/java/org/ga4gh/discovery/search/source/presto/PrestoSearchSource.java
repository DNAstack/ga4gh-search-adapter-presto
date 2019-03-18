/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.source.presto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Field.Type;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.result.ResultRow;
import org.ga4gh.discovery.search.result.ResultValue;
import org.ga4gh.discovery.search.result.SearchResult;
import org.ga4gh.discovery.search.source.SearchSource;
import org.ga4gh.discovery.search.source.presto.convert.PrestoSearchQueryConverter;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author mfiume
 */
public class PrestoSearchSource implements SearchSource {

    private final String driverClassName;
    private final String url;
    private final String username;
    private final String password;
    private Map<String,Field> fieldMap;
    private List<Field> fields;
    
    @Value( "${presto.results.limit.max}" )
    private int maxResultsLimit;

    private final static String TABLE = "\"drs\".\"org_ga4gh_drs\".\"objects\"";
            
    public PrestoSearchSource(String url, String username, String password, String driverClassName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
    }
    
    private List<Field> updateFields() {
        try {
            fieldMap = new TreeMap<String,Field>();
            
            Connection connection = getConnection();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("show columns from " + TABLE);  

            while (rs.next()) {
                String fieldName = rs.getString(1);
                Field.Type fieldType = prestoToPrimativeType(rs.getString(2));
                Field field = new Field(
                        TABLE + ".\"" + fieldName + "\"",
                        fieldName,
                        fieldType,
                        operatorsForType(fieldType), 
                        new String[0],
                        TABLE
                );
                fieldMap.put(field.getId(),field);
            }
            
            connection.close();
            
            List<Field> fields = new ArrayList<>();
            fields.addAll(fieldMap.values());
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
        
        try {
            System.out.println("Received query: " + query);
            query.setFrom(TABLE);
            
            if (query.getLimit() > maxResultsLimit) {
                throw new UnsupportedOperationException("Limit " + query.getLimit() + " exceeds maximum limit of " + maxResultsLimit);
            }
            
            if (query.getLimit() == 0) {
                query.setLimit(maxResultsLimit);
            }
            
            String prestoSqlString = PrestoSearchQueryConverter.convertSearchQueryToPrestoSQL(query);
            System.out.println(prestoSqlString);
            
            Connection connection = getConnection();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(prestoSqlString);
            
            List<ResultRow> results = new ArrayList<>();
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int numFields = rsmd.getColumnCount();
            
            List<Field> resultFields = new ArrayList<Field>();
            for (int i = 1; i <= numFields; i++) {
                String fieldName = rsmd.getColumnName(i);
                Type type = prestoToPrimativeType(rsmd.getColumnTypeName(i));
                resultFields.add(
                        new Field(
                                TABLE + ".\"" + fieldName + "\"",
                                fieldName,
                                type,
                                operatorsForType(type),
                                new String[0],
                                TABLE
                        ));
            }
            
            while (rs.next()) {
                List<ResultValue> values = new ArrayList<>();
                
                for (int i = 1; i <= numFields; i++) {     
                    values.add(new ResultValue(
                        resultFields.get(i-1), 
                        rs.getString(i)));
                }
                
                results.add(new ResultRow(values));
            }
            
            SearchResult searchResult = new SearchResult(resultFields,results);
            return searchResult;
            
        } catch (SQLException ex) {
            Logger.getLogger(PrestoSearchSource.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
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
        } else if (prestoType.startsWith("array(varchar")) {
            return Field.Type.STRING_ARRAY;
        } else if (prestoType.startsWith("array(row")) {
            return Field.Type.JSON;
        }
        throw new RuntimeException("Unknown mapping for Presto field type " + prestoType);
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                    url,
                    username,
                    password);
    }
    
    
    
}
