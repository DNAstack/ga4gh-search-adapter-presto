package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.Type;
import com.dnastack.ga4gh.search.adapter.model.ResultRow;
import com.dnastack.ga4gh.search.adapter.model.ResultValue;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.NonNull;

public class PagingResultSetConsumer {

    private final int pageSize;
    private final String consumerId;

    private ResultSet resultSet;
    private int currentPage = 0;
    private List<Field> fields;
    private boolean hasNext;
    private String nextPageToken;
    private List<String> consumedPages;

    public PagingResultSetConsumer(ResultSet resultSet, int pageSize) throws SQLException {
        this.consumerId = UUID.randomUUID().toString();
        this.nextPageToken = UUID.randomUUID().toString();
        this.resultSet = resultSet;
        this.pageSize = pageSize;
        this.hasNext = true;
        this.consumedPages = new ArrayList<>();
        this.fields = extractFields();
    }

    public List<Field> getFields() {
        return fields;
    }

    public boolean hasNextPage() {
        return hasNext;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void close() {
        try {
            if (!resultSet.isClosed()) {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public PageResult firtsPage() throws SQLException {
        if (currentPage == 0) {
            return nextPage(this.nextPageToken);
        } else {
            throw new SQLException("Cannot retrieve first page. Page has already been consumed");
        }
    }

    public PageResult nextPage(@NonNull String verificationToken) throws SQLException {
        if (hasNextPage()) {
            verifyNextPageToken(verificationToken);
            this.consumedPages.add(this.nextPageToken);
            this.nextPageToken = UUID.randomUUID().toString();
            List<ResultRow> pageRows = getPageRows();
            return new PageResult(this.consumerId, hasNext ? this.nextPageToken : null, fields, pageRows);
        }
        throw new SQLException("Result set has been fully consumed");
    }

    public void consumeAll(Consumer<ResultSet> consumer) {
        consumer.accept(resultSet);
        this.hasNext = false;
        this.currentPage = -1;
    }

    private void verifyNextPageToken(String verificationToken) throws SQLException {
        if (consumedPages.contains(verificationToken)) {
            throw new SQLException("Could not fetch desired page. Page has already been consumed");
        }

        if (!verificationToken.equals(nextPageToken)) {
            throw new SQLException("Could not fetch next page, next page token does not match expected value");
        }
    }

    private List<ResultRow> getPageRows() throws SQLException {
        int rowCounter = 0;
        List<ResultRow> resultRows = new ArrayList<>();
        boolean next = true;
        while (rowCounter < pageSize && next) {
            next = resultSet.next();
            if (next) {
                resultRows.add(extractRow(resultSet, fields));
                rowCounter++;
            }
        }
        hasNext = resultRows.size() == pageSize && next;
        currentPage++;
        return resultRows;
    }

    private List<Field> extractFields() throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Field> fields = new ArrayList<>(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            String prestoType = resultSetMetaData.getColumnTypeName(i);
            Type primitiveType = Metadata.prestoToPrimitiveType(prestoType);
            String[] typeOperators = Metadata.operatorsForType(primitiveType);
            //TODO: This data is not populated correctly -- why?
            String qualifiedTableName = String.format("%s.%s.%s",
                resultSetMetaData.getCatalogName(i),
                resultSetMetaData.getSchemaName(i),
                resultSetMetaData.getTableName(i));
            String id = qualifiedTableName + "." + columnName;
            //TODO: Temporary workaround while above is unpopulated
            Field f = new Field(columnName, columnName, primitiveType, typeOperators, null, qualifiedTableName);
            //Field f = new Field(id, columnName, primitiveType, typeOperators, null, qualifiedTableName);
            fields.add(f);
        }

        return fields;
    }

    public static ResultRow extractRow(ResultSet resultSet, List<Field> fields) throws SQLException {
        List<ResultValue> values = new ArrayList<>();
        for (int i = 1; i <= fields.size(); i++) {
            values.add(new ResultValue(fields.get(i - 1), resultSet.getString(i)));
        }
        return new ResultRow(values);
    }

}
