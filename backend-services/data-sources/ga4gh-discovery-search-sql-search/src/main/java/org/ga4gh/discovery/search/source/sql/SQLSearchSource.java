/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.source.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
public class SQLSearchSource implements SearchSource {

    private final String driverClassName;
    private final String url;
    private final String username;
    private final String password;
    private List<Field> fields;

    public SQLSearchSource(String url, String username, String password, String driverClassName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
    }
    
    private List<Field> updateFields() {
        try {
            List<Field> fields = new ArrayList<Field>();
              
            Class.forName(driverClassName);
            Connection connection = DriverManager.getConnection(
                    url,
                    username,
                    password);
            Statement stmt = connection.createStatement();
            ResultSet rs=stmt.executeQuery("DESC subject");  
            while(rs.next()) {
                String fieldId = rs.getString("field"); 
                Field.Type fieldType = sqlToPrimativeType(rs.getString("type"));
               
                fields.add(new Field(
                        fieldId,
                        fieldId,
                        fieldType,
                        operatorsForType(fieldType), 
                        new String[0],
                        "subject"
                ));
            }
            connection.close();
            return fields;
        } catch (Exception ex) {
            Logger.getLogger(SQLSearchSource.class.getName()).log(Level.SEVERE, null, ex);
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
        List<ResultRow> results = new ArrayList<ResultRow>();
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
            default :
                return new String[0];
        }
    }
    
    
    
}
