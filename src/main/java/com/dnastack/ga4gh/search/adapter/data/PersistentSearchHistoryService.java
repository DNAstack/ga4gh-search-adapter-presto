package com.dnastack.ga4gh.search.adapter.data;

import com.dnastack.ga4gh.search.adapter.model.Pagination;
import com.nimbusds.jose.JOSEException;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jdbi.v3.core.Jdbi;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.oauth2.jwt.Jwt;

@Slf4j
public class PersistentSearchHistoryService implements SearchHistoryService {

    private final Jdbi jdbi;
    private final EncryptionService encryptionService;


    public PersistentSearchHistoryService(Jdbi jdbi) {
        this.encryptionService = null;
        this.jdbi = jdbi;
    }

    public PersistentSearchHistoryService(Jdbi jdbi, EncryptionService encryptionService) {
        this.jdbi = jdbi;
        this.encryptionService = encryptionService;

    }


    @Override
    public void addSearchHistory(String sql, Boolean succeeded) {
        String userId = getUserId();
        if (userId != null) {
            SearchHistory searchHistory = new SearchHistory();
            searchHistory.setUserId(getUserId());
            searchHistory.setSqlQuery(sql);
            searchHistory.setSubmissionDate(ZonedDateTime.now());
            searchHistory.setSucceeded(Optional.ofNullable(succeeded).orElse(false));
            potentiallyEncryptSearch(searchHistory);
            jdbi.withExtension(SearchHistoryDao.class, dao -> {
                dao.saveSearch(searchHistory);
                return null;
            });

        } else {
            log.warn("Request is not authenticated with a userId, cannot save search history");
            return;
        }
    }

    private SearchHistory potentiallyEncryptSearch(SearchHistory searchHistory) {
        if (encryptionService != null) {
            try {
                searchHistory.setSqlQuery(encryptionService.encrypt(searchHistory.getSqlQuery()));
            } catch (JOSEException e) {
                log.error("Could not encrypt SQL string");
            }
        }
        return searchHistory;
    }

    private SearchHistory potentiallyDecryptSearch(SearchHistory searchHistory) {
        if (encryptionService != null) {
            try {
                searchHistory.setSqlQuery(encryptionService.decrypt(searchHistory.getSqlQuery()));
            } catch (Exception e) {
                log.debug("Could not decrypt SQL string:  " + e.getMessage());
            }
        }
        return searchHistory;
    }

    @Override
    public ListSearchHistory getSearchHistory(Integer page, Integer pageSize) {
        if (page == null) {
            page = 0;
        }
        if (pageSize == null) {
            pageSize = SearchHistoryService.DEFAULT_PAGE_SIZE;
        }

        int finalPage = page;
        int finalPageSize = pageSize;

        ListSearchHistory searchHistory = new ListSearchHistory();
        String userId = getUserId();
        if (userId != null) {
            return jdbi.withExtension(SearchHistoryDao.class, dao -> {
                int offset = finalPage * finalPageSize;
                int limit = finalPageSize;
                int totalResults = dao.countUsersSearchRequest(userId);
                List<SearchHistory> histories = dao.getSearchHistories(userId, offset, limit).stream()
                    .map(this::potentiallyDecryptSearch).collect(Collectors.toList());
                Pagination pagination = new Pagination();

                if (totalResults > offset && finalPage > 0) {
                    URI previous = URI
                        .create("history?" + String.format("page=%d&page_size=%d", finalPage - 1, finalPageSize));
                    pagination.setPreviousPageUrl(previous);
                }

                if (totalResults > offset + limit) {
                    URI next = URI
                        .create("history?" + String.format("page=%d&page_size=%d", finalPage + 1, finalPageSize));
                    pagination.setNextPageUrl(next);
                }

                searchHistory.setPagination(pagination);
                searchHistory.setSearchHistory(histories);
                return searchHistory;
            });
        } else {
            log.warn("Request is not authenticated with a userId, cannot retrieve search history");
            searchHistory.setSearchHistory(Collections.emptyList());
            return searchHistory;
        }
    }

    private String getUserId() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.isAuthenticated()) {
            Object principal = authentication.getPrincipal();
            if (principal instanceof User) {
                return ((User) principal).getUsername();
            } else if (principal instanceof Jwt) {
                return ((Jwt) principal).getSubject();
            } else {
                return principal.toString();
            }
        } else {
            return null;
        }
    }
}
