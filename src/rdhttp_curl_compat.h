/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * ClickHouse build: minimal libcurl-compatible declarations.
 *
 * librdkafka's HTTP client (needed for OAUTHBEARER/OIDC) is built on top of
 * Poco instead of libcurl; see contrib/librdkafka-cmake/rdhttp_poco.c. libcurl
 * is neither included nor linked. These declarations provide the handful of
 * curl types and functions that the OIDC code (rdkafka_sasl_oauthbearer_oidc.c)
 * and the HTTP request API (rdhttp.h) still refer to by name. The functions are
 * implemented in rdhttp_poco.c.
 */

#ifndef _RDHTTP_CURL_COMPAT_H_
#define _RDHTTP_CURL_COMPAT_H_

/** Opaque handle type; only ever used as a pointer. */
typedef void CURL;

/** Size of the per-request error buffer (matches libcurl's CURL_ERROR_SIZE). */
#define CURL_ERROR_SIZE 256

/** Singly-linked list of strings, as used by libcurl for HTTP headers. */
struct curl_slist {
        char *data;
        struct curl_slist *next;
};

struct curl_slist *curl_slist_append(struct curl_slist *list, const char *data);
void curl_slist_free_all(struct curl_slist *list);
char *curl_easy_escape(CURL *handle, const char *string, int length);
void curl_free(void *p);

#endif /* _RDHTTP_CURL_COMPAT_H_ */
