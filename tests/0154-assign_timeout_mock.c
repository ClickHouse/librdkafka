/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, ClickHouse Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"
#include "rdkafka.h"

/**
 * @name Verify that rd_kafka_assign() returns within a bounded time
 *       even when the group coordinator / cgrp state machine is
 *       unable to make progress.
 *
 * Background:
 *   rd_kafka_assign() is called from the rebalance callback on the
 *   application thread.  Internally it sends an RD_KAFKA_OP_ASSIGN to
 *   the cgrp ops queue and waits for a reply.  Previously the wait
 *   used RD_POLL_INFINITE, meaning the application thread could hang
 *   indefinitely inside rd_kafka_consumer_poll() if the cgrp could
 *   not service the op in a timely manner (e.g. coordinator
 *   unreachable, queue forwarding stalled during destroy, etc.).
 *
 *   The fix introduces a 30 s timeout in rd_kafka_assign0().  On
 *   expiry the call returns RD_KAFKA_RESP_ERR__TIMED_OUT instead of
 *   hanging forever, letting the caller honour its own poll timeout
 *   and react to shutdown signals.
 *
 *   This test exercises rd_kafka_assign() from a rebalance callback
 *   while the cluster is repeatedly being torn down and brought back
 *   up.  It does not attempt to deterministically reproduce the
 *   original hang (which depends on a race in the main rdkafka
 *   thread) — instead it asserts the *invariant* that no individual
 *   rd_kafka_assign() call ever exceeds the timeout cap by more than
 *   a small slack.  Without the patch, if the hang is hit, the test
 *   blocks past TEST_TIMEOUT and fails; with the patch the assert on
 *   `max_assign_us` enforces the upper bound on every call.
 */

#define ASSIGN_TIMEOUT_MS 30000
#define ASSIGN_SLACK_MS   5000
#define ASSIGN_MAX_US     ((ASSIGN_TIMEOUT_MS + ASSIGN_SLACK_MS) * 1000)

static int64_t max_assign_us = 0;
static int     rebalance_cnt = 0;

static void rebalance_cb(rd_kafka_t *rk,
                         rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *parts,
                         void *opaque) {
        int64_t t0, dt;
        rd_kafka_resp_err_t aerr;

        rebalance_cnt++;
        TEST_SAY("Rebalance #%d: %s: %d partition(s)\n", rebalance_cnt,
                 rd_kafka_err2name(err), parts->cnt);

        t0 = test_clock();

        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS)
                aerr = rd_kafka_assign(rk, parts);
        else
                aerr = rd_kafka_assign(rk, NULL);

        dt = test_clock() - t0;

        if (dt > max_assign_us)
                max_assign_us = dt;

        TEST_SAY("rd_kafka_assign() returned %s in %" PRId64 "ms\n",
                 rd_kafka_err2name(aerr), dt / 1000);

        TEST_ASSERT(dt < ASSIGN_MAX_US,
                    "rd_kafka_assign() blocked for %" PRId64
                    "ms, exceeds bounded timeout of %dms (+%dms slack)",
                    dt / 1000, ASSIGN_TIMEOUT_MS, ASSIGN_SLACK_MS);

        /* TIMED_OUT is the expected error from the bounded wait.  Any
         * other error (or success) is also acceptable — we only care
         * that the call returned. */
        if (aerr && aerr != RD_KAFKA_RESP_ERR__TIMED_OUT)
                TEST_SAY("Non-timeout assign error (acceptable): %s\n",
                         rd_kafka_err2name(aerr));
}


/**
 * @brief Drive a consumer through rebalances while the brokers
 *        flicker up/down, and verify rd_kafka_assign() always
 *        returns within the bounded timeout.
 */
static void do_test_assign_bounded_timeout(void) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid = "bounded-assign-grp";
        const char *topic   = "bounded-assign-topic";
        int i;

        SUB_TEST_QUICK();

        rebalance_cnt = 0;
        max_assign_us = 0;

        mcluster = test_mock_cluster_new(3, &bootstraps);
        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);
        rd_kafka_mock_topic_create(mcluster, topic, 4, 1);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "partition.assignment.strategy", "range");

        c = test_create_consumer(groupid, rebalance_cb, conf, NULL);
        test_consumer_subscribe(c, topic);

        /* Poll until we have at least one assignment rebalance. */
        TEST_SAY("Waiting for initial assignment\n");
        while (rebalance_cnt == 0)
                test_consumer_poll_once(c, NULL, 500);

        /* Now flicker the brokers up/down a few times while polling.
         * This stresses the cgrp state machine: coordinator lookup,
         * group re-join, partition revocation/assignment all happen
         * in rapid succession.  Each rebalance invokes the callback
         * which times rd_kafka_assign(). */
        for (i = 0; i < 6; i++) {
                if (i % 2 == 0) {
                        TEST_SAY("Iteration %d: bringing all brokers down\n",
                                 i);
                        rd_kafka_mock_broker_set_down(mcluster, -1);
                } else {
                        TEST_SAY("Iteration %d: bringing all brokers up\n", i);
                        rd_kafka_mock_broker_set_up(mcluster, -1);
                }

                /* Poll for ~3 s — this is the application-visible poll
                 * timeout we are protecting.  Without the fix, a stuck
                 * cgrp would make this iteration take far longer. */
                test_consumer_poll_no_msgs("flicker", c, 0, 3000);
        }

        /* Final state: brokers up, drain any pending rebalances. */
        rd_kafka_mock_broker_set_up(mcluster, -1);
        test_consumer_poll_no_msgs("settle", c, 0, 3000);

        TEST_SAY("Total rebalance callbacks: %d, max assign() duration: "
                 "%" PRId64 "ms\n",
                 rebalance_cnt, max_assign_us / 1000);

        TEST_ASSERT(rebalance_cnt >= 1,
                    "Expected at least one rebalance callback, got %d",
                    rebalance_cnt);
        TEST_ASSERT(max_assign_us < ASSIGN_MAX_US,
                    "Maximum rd_kafka_assign() duration %" PRId64
                    "ms exceeds bounded timeout %dms (+%dms slack)",
                    max_assign_us / 1000, ASSIGN_TIMEOUT_MS, ASSIGN_SLACK_MS);

        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0154_assign_timeout_mock(int argc, char **argv) {
        if (test_needs_auth()) {
                TEST_SAY("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_assign_bounded_timeout();

        return 0;
}
