// T0 rows of the engine status feed: parsing a node's /health and /metrics, role derivation, the unreachable node read
// from its own failed poll (never from the leader), snapshot agreement (matched, diverged), the rate cap, change
// detection, null fields, election events, the bridge line, the frames in StreamCore's connection queues, and the poller
// against real loopback endpoints (an answering node, a refused port, a node that never answers).

#include <dlfcn.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>
#include <boost/json.hpp>

#include "EnginePoller.h"
#include "EngineStatus.h"
#include "StreamRig.h"

using namespace gwtest::stream_rig;
namespace en = openexchange::gateway::stream::engine;

namespace {

std::string health(const char *role, std::int64_t term, std::int64_t applied, std::int64_t commit,
    const char *recovery = "ready")
{
    return std::string("role=") + role + " recovery=" + recovery + " source=12 term=" + std::to_string(term) +
        " elections=1 applied=" + std::to_string(applied) + " commit=" + std::to_string(commit) + " checkpoint=" +
        std::to_string(commit) + " observationAgeMs=2 consensusAgeMs=3 maxLagBytes=65536 maxCheckpointAgeMs=1000\n";
}

// The /health body of an engine that names its release: the identity tokens come after the existing ones.
std::string healthWithIdentity(const char *role, std::int64_t term, std::int64_t applied, std::int64_t commit,
    const std::string &version, const std::string &build)
{
    std::string body = health(role, term, applied, commit);
    body.pop_back(); // the trailing newline
    return body + " version=" + version + " build=" + build + "\n";
}

std::string coreMetrics(std::int64_t member, std::int64_t snapshotPosition = -1, std::int64_t snapshotAge = -1,
    const std::string &extra = "")
{
    return "# HELP core_accounts Accounts in the ledger\n# TYPE core_accounts gauge\ncore_accounts 12\n"
           "# HELP core_sessions Registered live sessions\n# TYPE core_sessions gauge\ncore_sessions{class=\"BRIDGE\"} 2\n"
           "core_feed_cursor 100\n"
           "core_cluster_role 2\ncore_member_id " + std::to_string(member) + "\ncore_log_position 4096\n"
           "core_apply_dispatch_seconds_sum 1.5E-4\n"
           "core_snapshot_age_seconds " + std::to_string(snapshotAge) + "\ncore_snapshot_last_log_position " +
        std::to_string(snapshotPosition) + "\n" + extra;
}

en::NodeSample answered(const std::string &healthBody, const std::string &metricsBody, int healthStatus = 200)
{
    en::NodeSample s;
    s.healthAnswered = true;
    s.healthStatus = healthStatus;
    s.healthBody = healthBody;
    s.metricsAnswered = true;
    s.metricsStatus = 200;
    s.metricsBody = metricsBody;
    return s;
}

en::NodeSample silent(const char *why = "connect: Connection refused")
{
    en::NodeSample s;
    s.error = why;
    return s;
}

std::vector<en::NodeEndpointSpec> three()
{
    return { { 0, "10.0.0.1", 9700 }, { 1, "10.0.0.2", 9701 }, { 2, "10.0.0.3", 9702 } };
}

json::object parsed(const std::string &frame)
{
    return json::parse(frame).as_object();
}

const en::NodeView &node(const en::ClusterModel &m, std::int64_t id)
{
    for (const en::NodeView &n : m.nodes())
    {
        if (n.id == id)
        {
            return n;
        }
    }
    static en::NodeView none;
    return none;
}

}

TEST(engine_parse_health)
{
    const auto h = en::parseHealth(health("FOLLOWER", 3, 4000, 4096, "catching-up"));
    CHECK(h.has_value());
    if (h)
    {
        CHECK("FOLLOWER" == h->role.value_or(""));
        CHECK("catching-up" == h->recovery.value_or(""));
        CHECK(3 == h->term.value_or(-9));
        CHECK(1 == h->elections.value_or(-9));
        CHECK(4000 == h->applied.value_or(-9));
        CHECK(4096 == h->commit.value_or(-9));
    }
    // the probe writes -1 for unknown: null, never a number
    const auto unknown = en::parseHealth("role=CANDIDATE recovery=election source=-1 term=-1 elections=-1 applied=-1 commit=-1");
    CHECK(unknown && !unknown->term && !unknown->applied && !unknown->commit && "election" == unknown->recovery.value_or(""));
    CHECK(!en::parseHealth("ok\n").has_value());
    CHECK(!en::parseHealth("").has_value());
    const auto junk = en::parseHealth("role=LEADER term=x commit=12abc applied==5");
    CHECK(junk && !junk->term && !junk->commit && !junk->applied);
    // an engine that sends no identity: null, and every other field as before
    const auto bare = en::parseHealth(health("LEADER", 3, 4000, 4096));
    CHECK(bare && !bare->version && !bare->build && 4096 == bare->commit.value_or(-9));
    // version= and build= come after the existing tokens; commit= stays the commit log position even when the build
    // sha starts with digits
    const auto named = en::parseHealth(
        health("LEADER", 3, 4000, 4096) + "version=1.2.3-rc.1+meta build=4096abcdef");
    CHECK(named && "1.2.3-rc.1+meta" == named->version.value_or("") && "4096abcdef" == named->build.value_or("") &&
        4096 == named->commit.value_or(-9));
    const auto both = en::parseHealth("role=LEADER recovery=ready term=3 applied=4000 commit=4096 version=0.1.22 build=1234abc\n");
    CHECK(both && "0.1.22" == both->version.value_or("") && "1234abc" == both->build.value_or("") &&
        4096 == both->commit.value_or(-9));
    // the sanitized placeholder the engine writes when it does not know is shown as written
    const auto unknown2 = en::parseHealth("role=LEADER commit=7 version=unknown build=unknown");
    CHECK(unknown2 && "unknown" == unknown2->version.value_or("") && 7 == unknown2->commit.value_or(-9));
    // a key without a value is null, never an empty string
    const auto empty = en::parseHealth("role=LEADER version= build=");
    CHECK(empty && !empty->version && !empty->build);
}

TEST(engine_parse_metrics)
{
    const auto m = en::parseMetrics(coreMetrics(1, 8192, 30) + "core_x 5 1700000000\r\ncore_neg -1\nbroken\n");
    CHECK(12 == en::metricInt(m, "core_accounts").value_or(-9));
    CHECK(1 == en::metricInt(m, "core_member_id").value_or(-9));
    CHECK(8192 == en::metricInt(m, "core_snapshot_last_log_position").value_or(-9));
    CHECK(5 == en::metricInt(m, "core_x").value_or(-9)); // a timestamp after the value is not the value
    CHECK(-1 == en::metricInt(m, "core_neg").value_or(-9));
    CHECK(!en::metricInt(m, "core_apply_dispatch_seconds_sum")); // not an integer
    CHECK(!en::metricInt(m, "core_sessions"));                    // labelled series are not read
    CHECK(!en::metricInt(m, "core_absent"));
    CHECK(0 == m.count("# HELP core_accounts"));
}

TEST(engine_role_derivation)
{
    en::ClusterModel core(en::Flavor::CORE, three());
    core.ingest({ answered(health("FOLLOWER", 3, 4000, 4096), coreMetrics(0)),
                    answered(health("LEADER", 3, 4096, 4096), coreMetrics(1)),
                    answered(health("CANDIDATE", 2, 1000, 1000, "election"), coreMetrics(2), 503) },
        1000);
    CHECK(1 == core.leaderId().value_or(-9));
    CHECK(3 == core.term().value_or(-9));
    CHECK("follower" == node(core, 0).role.value_or(""));
    CHECK("leader" == node(core, 1).role.value_or(""));
    CHECK("candidate" == node(core, 2).role.value_or(""));
    CHECK(96 == node(core, 0).behindBytes.value_or(-9));
    CHECK(0 == node(core, 1).behindBytes.value_or(-9));
    CHECK(3096 == node(core, 2).behindBytes.value_or(-9));
    CHECK(true == node(core, 0).live.value_or(false));
    CHECK(false == node(core, 2).live.value_or(true)); // /health 503: the node's own duty cycle is not live
    CHECK(4096 == node(core, 0).commitPosition.value_or(-9));
    CHECK(4000 == node(core, 0).appliedPosition.value_or(-9));
    // a node polled after the leader can be ahead of the leader's polled commit: the raw difference is shown (the two
    // positions are sampled from two processes, not at one instant), never clamped into a made-up 0
    core.ingest({ answered(health("FOLLOWER", 3, 5000, 5000), coreMetrics(0)),
                    answered(health("LEADER", 3, 4096, 4096), coreMetrics(1)),
                    answered(health("FOLLOWER", 3, 4096, 4096), coreMetrics(2)) },
        2000);
    CHECK_MSG(-904 == node(core, 0).behindBytes.value_or(-9),
        "behindBytes clamped: " + std::to_string(node(core, 0).behindBytes.value_or(-9)));
    // two leaders: the higher term wins; the same term twice: no leader (a conflict, not a guess)
    core.ingest({ answered(health("LEADER", 4, 10, 10), coreMetrics(0)), answered(health("LEADER", 3, 4096, 4096), coreMetrics(1)),
                    answered(health("FOLLOWER", 4, 10, 10), coreMetrics(2)) },
        3000);
    CHECK(0 == core.leaderId().value_or(-9) && 4 == core.term().value_or(-9));
    core.ingest({ answered(health("LEADER", 4, 10, 10), coreMetrics(0)), answered(health("LEADER", 4, 10, 10), coreMetrics(1)),
                    answered(health("FOLLOWER", 4, 10, 10), coreMetrics(2)) },
        4000);
    CHECK(!core.leaderId() && !core.term());
    CHECK(!node(core, 2).behindBytes); // no leader: nothing to be behind of
}

// A node whose own /health is not live (503: its duty cycle is stale) keeps the role it last had evidence for; a stuck
// ex-leader's LEADER claim is shown on its row but never becomes the cluster's leader.
TEST(engine_leader_must_be_live)
{
    en::ClusterModel core(en::Flavor::CORE, three());
    core.ingest({ answered(health("LEADER", 3, 4096, 4096), coreMetrics(0), 503),
                    answered(health("FOLLOWER", 3, 4096, 4096), coreMetrics(1)), silent() },
        1000);
    CHECK("leader" == node(core, 0).role.value_or("") && false == node(core, 0).live.value_or(true));
    CHECK_MSG(!core.leaderId(), "stale leader claim from a node that is not live: leaderId " +
            std::to_string(core.leaderId().value_or(-9)));
    CHECK(!core.term());
    CHECK(!node(core, 1).behindBytes); // no live leader: nothing to be behind of
    // a live leader of a higher term elsewhere is the leader; the stale one stays on its own row
    core.ingest({ answered(health("LEADER", 3, 4096, 4096), coreMetrics(0), 503),
                    answered(health("FOLLOWER", 4, 4200, 4200), coreMetrics(1)),
                    answered(health("LEADER", 4, 4200, 4200), coreMetrics(2)) },
        2000);
    CHECK(2 == core.leaderId().value_or(-9) && 4 == core.term().value_or(-9));
}

// A follower that does not answer is unreachable from its own failed poll, whatever the leader reports.
TEST(engine_unreachable_node_from_its_own_poll)
{
    en::ClusterModel core(en::Flavor::CORE, three());
    core.ingest({ answered(health("LEADER", 3, 4096, 4096), coreMetrics(0)),
                    answered(health("FOLLOWER", 3, 4096, 4096), coreMetrics(1)), silent() },
        1000);
    const en::NodeView &n = node(core, 2);
    CHECK_MSG("unreachable" == n.role.value_or(""), "follower health inferred from the leader: node 2 role " +
            n.role.value_or("null"));
    CHECK(n.error && std::string::npos != n.error->find("refused"));
    CHECK(!n.commitPosition && !n.appliedPosition && !n.term && !n.live && !n.behindBytes);
    CHECK("follower" == node(core, 1).role.value_or(""));
    CHECK(0 == core.leaderId().value_or(-9)); // the leader is still the leader
    const json::object f = parsed(en::engineFrame(core, en::ClusterModel(en::Flavor::MATCHING_ENGINE, {})));
    const json::array &nodes = f.at("core").at("nodes").as_array();
    CHECK(3 == nodes.size());
    if (3 == nodes.size())
    {
        CHECK("unreachable" == nodes[2].at("role").as_string());
        CHECK(nodes[2].at("commitPosition").is_null());
    }
}

// Anything between answering and not answering is shown with no values that might be another node's.
TEST(engine_partial_or_foreign_answer_shows_no_values)
{
    en::ClusterModel core(en::Flavor::CORE, three());
    en::NodeSample onlyHealth = answered(health("FOLLOWER", 3, 4096, 4096), "");
    onlyHealth.metricsAnswered = false;
    onlyHealth.error = "no answer within 500 ms";
    core.ingest({ answered(health("LEADER", 3, 4096, 4096), coreMetrics(0)),
                    answered(health("FOLLOWER", 3, 4096, 4096), coreMetrics(7)), onlyHealth },
        1000);
    CHECK(!node(core, 1).role && node(core, 1).error && std::string::npos != node(core, 1).error->find("member id 7"));
    CHECK(!node(core, 1).commitPosition);
    CHECK(!node(core, 2).role && node(core, 2).error && std::string::npos != node(core, 2).error->find("/metrics"));
    CHECK(!node(core, 2).term);
    en::ClusterModel other(en::Flavor::CORE, three());
    other.ingest({ answered("ok\n", coreMetrics(0)), answered(health("FOLLOWER", 3, 1, 1), "nothing here\n"),
                     answered(health("LEADER", 3, 1, 1), coreMetrics(2), 200) },
        1000);
    CHECK(!node(other, 0).role && node(other, 0).error);
    CHECK(!node(other, 1).role && node(other, 1).error); // no member id: not confirmed
    CHECK("leader" == node(other, 2).role.value_or(""));
}

TEST(engine_snapshot_matched_and_diverged)
{
    const std::string hashA = "core_snapshot_state_hash 18446744073709551615\n";
    const std::string hashB = "core_snapshot_state_hash 42\n";
    en::ClusterModel core(en::Flavor::CORE, three());
    core.ingest({ answered(health("LEADER", 3, 9000, 9000), coreMetrics(0, 8192, 10, hashA)),
                    answered(health("FOLLOWER", 3, 9000, 9000), coreMetrics(1, 8192, 10, hashA)),
                    answered(health("FOLLOWER", 3, 9000, 9000), coreMetrics(2, 8192, 11, hashA)) },
        100'000);
    CHECK(8192 == core.snapshot().position.value_or(-9));
    CHECK(90'000 == core.snapshot().atMs.value_or(-9));
    CHECK("matched" == core.snapshot().agreement.value_or(""));
    CHECK(core.divergedNodes().empty());
    CHECK("matched" == node(core, 2).snapshot.agreement.value_or(""));
    core.ingest({ answered(health("LEADER", 3, 9000, 9000), coreMetrics(0, 8192, 11, hashA)),
                    answered(health("FOLLOWER", 3, 9000, 9000), coreMetrics(1, 8192, 11, hashB)),
                    answered(health("FOLLOWER", 3, 9000, 9000), coreMetrics(2, 8192, 12, hashA)) },
        101'000);
    CHECK("diverged" == core.snapshot().agreement.value_or(""));
    CHECK(1 == core.divergedNodes().size() && 1 == core.divergedNodes()[0]);
    CHECK("diverged" == node(core, 1).snapshot.agreement.value_or(""));
    CHECK("matched" == node(core, 0).snapshot.agreement.value_or(""));
    CHECK(90'000 == node(core, 0).snapshot.atMs.value_or(-9)); // fixed when first seen: the moving age does not move it
    const json::object f = parsed(en::engineFrame(core, en::ClusterModel(en::Flavor::MATCHING_ENGINE, {})));
    CHECK("diverged" == f.at("core").at("snapshot").at("agreement").as_string());
    CHECK(1 == f.at("core").at("snapshot").at("divergedNodes").as_array().size());
    // a node at another position is not compared; one hash alone decides nothing
    core.ingest({ answered(health("LEADER", 3, 9000, 9000), coreMetrics(0, 16384, 0, hashA)),
                    answered(health("FOLLOWER", 3, 9000, 9000), coreMetrics(1, 8192, 12, hashB)),
                    answered(health("FOLLOWER", 3, 9000, 9000), coreMetrics(2)) },
        102'000);
    CHECK(16384 == core.snapshot().position.value_or(-9));
    CHECK(!core.snapshot().agreement);
    CHECK(102'000 == core.snapshot().atMs.value_or(-9));
    // the pure rule with an even split: the lowest id's hash is the reference
    std::vector<en::NodeView> nodes(4);
    for (int i = 0; i < 4; ++i)
    {
        nodes[i].id = i;
        nodes[i].snapshot.position = 5;
        nodes[i].snapshot.stateHash = (i % 2) ? "b" : "a";
    }
    std::vector<std::int64_t> diverged;
    CHECK("diverged" == en::snapshotAgreement(nodes, 5, diverged).value_or(""));
    CHECK(2 == diverged.size() && 1 == diverged[0] && 3 == diverged[1]);
    CHECK(!en::snapshotAgreement(nodes, 6, diverged));
}

TEST(engine_snapshot_hash_is_digits_only)
{
    // an engine that cannot name its hash sends none, or a sentinel: neither may read as an equal hash
    const auto metrics = [](const char *hash) { return std::string("core_snapshot_state_hash ") + hash + "\n"; };
    en::ClusterModel core(en::Flavor::CORE, three());
    core.ingest({ answered(health("LEADER", 3, 9000, 9000), coreMetrics(0, 8192, 10, metrics("-1"))),
                    answered(health("FOLLOWER", 3, 9000, 9000), coreMetrics(1, 8192, 10, metrics("-1"))),
                    answered(health("FOLLOWER", 3, 9000, 9000), coreMetrics(2, 8192, 10, metrics("-1"))) },
        100'000);
    for (std::int64_t i = 0; i < 3; ++i)
    {
        CHECK_MSG(!node(core, i).snapshot.stateHash, "a negative sentinel is accepted as a snapshot hash");
    }
    CHECK_MSG(!core.snapshot().agreement, "three nodes reporting -1 read as matched");
    for (const char *bad : { "NaN", "0x2a", "12ab", "1.5", "+7", "1e3" })
    {
        en::ClusterModel one(en::Flavor::CORE, three());
        one.ingest({ answered(health("LEADER", 3, 9000, 9000), coreMetrics(0, 8192, 10, metrics(bad))), silent(), silent() },
            100'000);
        CHECK_MSG(!node(one, 0).snapshot.stateHash, std::string("accepted as a hash: ") + bad);
    }
    en::ClusterModel ok(en::Flavor::CORE, three());
    ok.ingest({ answered(health("LEADER", 3, 9000, 9000), coreMetrics(0, 8192, 10, metrics("0"))), silent(), silent() },
        100'000);
    CHECK("0" == node(ok, 0).snapshot.stateHash.value_or(""));
}

TEST(engine_matching_engine_snapshot_position_and_hash)
{
    const auto meMetrics = [](std::int64_t member, const char *position, const char *hash) {
        return "match_cluster_role 2\nmatch_member_id " + std::to_string(member) +
            "\nmatch_snapshot_age_seconds 60\nmatch_snapshot_last_log_position " + position +
            "\nmatch_snapshot_state_hash " + hash + "\nmatch_engine_next_journal_seq 151\n";
    };
    en::ClusterModel me(en::Flavor::MATCHING_ENGINE, { { 0, "10.0.1.1", 9500 }, { 1, "10.0.1.2", 9501 }, { 2, "10.0.1.3", 9502 } });
    me.ingest({ answered(health("LEADER", 7, 500, 500), meMetrics(0, "4096", "3735928559")),
                  answered(health("FOLLOWER", 7, 500, 500), meMetrics(1, "4096", "3735928559")),
                  answered(health("FOLLOWER", 7, 500, 500), meMetrics(2, "4096", "3735928559")) },
        200'000);
    CHECK(4096 == node(me, 0).snapshot.position.value_or(-9));
    CHECK("3735928559" == node(me, 1).snapshot.stateHash.value_or(""));
    CHECK(4096 == me.snapshot().position.value_or(-9));
    CHECK("matched" == me.snapshot().agreement.value_or(""));
    me.ingest({ answered(health("LEADER", 7, 500, 500), meMetrics(0, "4096", "3735928559")),
                  answered(health("FOLLOWER", 7, 500, 500), meMetrics(1, "4096", "1")),
                  answered(health("FOLLOWER", 7, 500, 500), meMetrics(2, "4096", "3735928559")) },
        201'000);
    CHECK("diverged" == me.snapshot().agreement.value_or(""));
    CHECK(1 == me.divergedNodes().size() && 1 == me.divergedNodes()[0]);
    // -1 = never taken: no position, and no hash
    me.ingest({ answered(health("LEADER", 7, 500, 500), meMetrics(0, "-1", "-1")), silent(), silent() }, 202'000);
    CHECK(!node(me, 0).snapshot.position && !node(me, 0).snapshot.stateHash);
}

TEST(engine_version_and_build_commit)
{
    en::ClusterModel core(en::Flavor::CORE, three());
    core.ingest({ answered(healthWithIdentity("LEADER", 3, 10, 10, "0.1.22", "1234abc"), coreMetrics(0, 8192, 5)),
                    answered(health("FOLLOWER", 3, 10, 10), coreMetrics(1, 8192, 5)), silent() },
        50'000);
    CHECK("0.1.22" == node(core, 0).version.value_or("") && "1234abc" == node(core, 0).buildCommit.value_or(""));
    CHECK(10 == node(core, 0).appliedPosition.value_or(-9) && 10 == node(core, 0).commitPosition.value_or(-9));
    CHECK(!node(core, 1).version && !node(core, 1).buildCommit);
    const json::object f = parsed(en::engineFrame(core, en::ClusterModel(en::Flavor::MATCHING_ENGINE, {})));
    const json::array &rows = f.at("core").at("nodes").as_array();
    CHECK("0.1.22" == rows.at(0).at("version").as_string() && "1234abc" == rows.at(0).at("buildCommit").as_string());
    CHECK(rows.at(1).at("version").is_null() && rows.at(1).at("buildCommit").is_null());
    // a node that does not answer shows no identity
    CHECK(!node(core, 2).version && !node(core, 2).buildCommit);
}

TEST(engine_null_fields)
{
    // nothing configured: both blocks all null, no node rows
    const en::ClusterModel none(en::Flavor::CORE, {});
    const en::ClusterModel noneMe(en::Flavor::MATCHING_ENGINE, {});
    const json::object f = parsed(en::engineFrame(none, noneMe));
    CHECK("engine" == f.at("type").as_string());
    for (const char *k : { "core", "matchingEngine" })
    {
        const json::object &b = f.at(k).as_object();
        CHECK(false == b.at("configured").as_bool());
        CHECK(b.at("term").is_null() && b.at("leaderId").is_null() && b.at("lastElection").is_null());
        CHECK(b.at("nodes").as_array().empty());
        CHECK(b.at("snapshot").at("position").is_null() && b.at("snapshot").at("agreement").is_null());
    }
    CHECK(f.at("bridge").at("consumedJournalSeq").is_null() && f.at("bridge").at("entriesBehind").is_null());
    // configured, never polled: a row per node, every value null
    const en::ClusterModel fresh(en::Flavor::CORE, three());
    const json::object g = parsed(en::engineFrame(fresh, noneMe));
    const json::array &rows = g.at("core").at("nodes").as_array();
    CHECK(3 == rows.size());
    for (const json::value &r : rows)
    {
        for (const char *k : { "role", "live", "term", "commitPosition", "appliedPosition", "behindBytes", "entriesBehind",
                 "version", "buildCommit", "error" })
        {
            CHECK_MSG(r.at(k).is_null(), std::string(k) + " is not null before the first poll");
        }
    }
    // polled and answering, an engine that sends no identity and no hash: those stay null
    en::ClusterModel core(en::Flavor::CORE, three());
    core.ingest({ answered(health("LEADER", 3, 10, 10), coreMetrics(0, 8192, 5)),
                    answered(health("FOLLOWER", 3, 10, 10), coreMetrics(1, 8192, 5)),
                    answered(health("FOLLOWER", 3, 10, 10), coreMetrics(2, 8192, 5)) },
        50'000);
    const json::object h = parsed(en::engineFrame(core, noneMe));
    for (const json::value &r : h.at("core").at("nodes").as_array())
    {
        CHECK(r.at("entriesBehind").is_null() && r.at("version").is_null() && r.at("buildCommit").is_null());
        CHECK(r.at("snapshot").at("stateHash").is_null() && r.at("snapshot").at("agreement").is_null());
        CHECK(8192 == r.at("snapshot").at("position").as_int64());
    }
    CHECK(h.at("core").at("snapshot").at("agreement").is_null());
}

TEST(engine_matching_engine_block)
{
    en::ClusterModel me(en::Flavor::MATCHING_ENGINE, { { 0, "10.0.1.1", 9500 }, { 1, "10.0.1.2", 9501 } });
    const std::string meMetrics0 = "match_cluster_role 2\nmatch_member_id 0\nmatch_snapshot_age_seconds 60\n"
                                   "match_engine_next_journal_seq 151\n";
    const std::string meMetrics1 = "match_cluster_role 0\nmatch_member_id 1\nmatch_snapshot_age_seconds -1\n"
                                   "match_engine_next_journal_seq 151\n";
    me.ingest({ answered(health("LEADER", 7, 500, 500), meMetrics0), answered(health("FOLLOWER", 7, 480, 500), meMetrics1) },
        200'000);
    CHECK(0 == me.leaderId().value_or(-9) && 7 == me.term().value_or(-9));
    CHECK(20 == node(me, 1).behindBytes.value_or(-9));
    CHECK(!node(me, 0).snapshot.position);                   // this engine sent no snapshot position series
    CHECK(140'000 == node(me, 0).snapshot.atMs.value_or(-9)); // its age is
    CHECK(!node(me, 1).snapshot.atMs);                       // -1 = never
    CHECK(140'000 == me.snapshot().atMs.value_or(-9));
    en::ClusterModel core(en::Flavor::CORE, three());
    core.ingest({ answered(health("LEADER", 3, 10, 10), coreMetrics(0)), answered(health("FOLLOWER", 3, 10, 10), coreMetrics(1)),
                    silent() },
        200'000);
    const json::object f = parsed(en::engineFrame(core, me));
    CHECK(100 == f.at("bridge").at("consumedJournalSeq").as_int64());
    CHECK(150 == f.at("bridge").at("meJournalSeq").as_int64());
    CHECK(50 == f.at("bridge").at("entriesBehind").as_int64());
    CHECK(true == f.at("matchingEngine").at("configured").as_bool());
}

TEST(engine_election_events)
{
    en::ClusterModel core(en::Flavor::CORE, three());
    auto round = [&](en::NodeSample a, en::NodeSample b, en::NodeSample c, std::int64_t now) {
        return core.ingest({ std::move(a), std::move(b), std::move(c) }, now);
    };
    // the first leader seen is not an election
    CHECK(round(answered(health("LEADER", 3, 4096, 4096), coreMetrics(0)), answered(health("FOLLOWER", 3, 4096, 4096), coreMetrics(1)),
              answered(health("FOLLOWER", 3, 4096, 4096), coreMetrics(2)), 1000)
              .empty());
    CHECK(!core.lastElection());
    // the leader dies: its followers still follow (no election yet), the dead leader's row is unreachable
    CHECK(round(silent(), answered(health("FOLLOWER", 3, 4096, 4096), coreMetrics(1)),
              answered(health("FOLLOWER", 3, 4096, 4096), coreMetrics(2)), 2000)
              .empty());
    // the followers say they are electing: start
    const auto start = round(silent(), answered(health("CANDIDATE", 3, 4096, 4096, "election"), coreMetrics(1)),
        answered(health("FOLLOWER", 3, 4096, 4096, "election"), coreMetrics(2)), 3000);
    CHECK(1 == start.size());
    if (1 == start.size())
    {
        CHECK("start" == start[0].phase);
        CHECK(0 == start[0].previousLeaderId.value_or(-9) && 3 == start[0].previousTerm.value_or(-9));
        CHECK(4096 == start[0].lastCommitPosition.value_or(-9));
        CHECK(1 == start[0].candidates.size() && 1 == start[0].candidates[0]);
        const json::object f = parsed(en::electionFrame("core", start[0]));
        CHECK("election" == f.at("type").as_string() && "core" == f.at("cluster").as_string());
        CHECK("start" == f.at("phase").as_string() && 4096 == f.at("lastCommitPosition").as_int64());
    }
    // still electing: no second start
    CHECK(round(silent(), answered(health("CANDIDATE", 4, 4096, 4096, "election"), coreMetrics(1)),
              answered(health("FOLLOWER", 4, 4096, 4096, "election"), coreMetrics(2)), 3500)
              .empty());
    // a new leader: end, with the old and new leader and term
    const auto end = round(silent(), answered(health("LEADER", 4, 4200, 4200), coreMetrics(1)),
        answered(health("FOLLOWER", 4, 4200, 4200), coreMetrics(2)), 4000);
    CHECK(1 == end.size());
    if (1 == end.size())
    {
        CHECK("end" == end[0].phase);
        CHECK(0 == end[0].previousLeaderId.value_or(-9) && 1 == end[0].leaderId.value_or(-9));
        CHECK(3 == end[0].previousTerm.value_or(-9) && 4 == end[0].term.value_or(-9));
        CHECK(3000 == end[0].observedGapMs.value_or(-9)); // last round that saw the old leader (1000) to this one
        const json::object f = parsed(en::electionFrame("core", end[0]));
        CHECK(f.at("durationMs").is_null() && f.at("firstEntryPosition").is_null());
        // order entry reopened: no source in oe-stream (absent -> an explicit null, never a missing key)
        const json::value *reopened = f.if_contains("orderEntryReopened");
        CHECK_MSG(nullptr != reopened && reopened->is_null(), "election end without an explicit orderEntryReopened null");
    }
    CHECK(core.lastElection() && 1 == core.lastElection()->leaderId.value_or(-9));
    const json::object frame = parsed(en::engineFrame(core, en::ClusterModel(en::Flavor::MATCHING_ENGINE, {})));
    CHECK(1 == frame.at("core").at("lastElection").at("leaderId").as_int64());
    CHECK(0 == frame.at("core").at("lastElection").at("previousLeaderId").as_int64());
    CHECK(frame.at("core").at("lastElection").at("durationMs").is_null());
    // a change of leader seen with no start in between (the election fell between two rounds): end only
    const auto direct = round(answered(health("LEADER", 5, 5000, 5000), coreMetrics(0)),
        answered(health("FOLLOWER", 5, 5000, 5000), coreMetrics(1)), answered(health("FOLLOWER", 5, 5000, 5000), coreMetrics(2)),
        5000);
    CHECK(1 == direct.size() && "end" == direct[0].phase && 0 == direct[0].leaderId.value_or(-9) &&
        1 == direct[0].previousLeaderId.value_or(-9));
    // a follower that rejoins in an election state while the leader still leads is not an election
    CHECK(round(answered(health("LEADER", 5, 5000, 5000), coreMetrics(0)),
              answered(health("FOLLOWER", 5, 10, 10, "election"), coreMetrics(1)),
              answered(health("FOLLOWER", 5, 5000, 5000), coreMetrics(2)), 6000)
              .empty());
}

TEST(engine_rate_cap)
{
    en::FrameGate gate(1000);
    CHECK(gate.offer("a", 0).value_or("") == "a"); // the first frame goes at once
    CHECK(!gate.offer("b", 100));                    // changed inside the cap: held
    CHECK_MSG(!gate.offer("c", 500), "engine frame rate above the cap");
    CHECK(!gate.poll(999));
    CHECK(gate.poll(1000).value_or("") == "c"); // the newest held frame, once the cap allows
    CHECK(!gate.poll(1500));
    // a steady change every 100 ms: at most one frame per second
    int sent = 0;
    for (std::int64_t t = 1100; t <= 11'000; t += 100)
    {
        if (gate.offer("f" + std::to_string(t), t))
        {
            sent++;
        }
    }
    CHECK_MSG(sent <= 10, "engine frame rate above the cap: " + std::to_string(sent) + " frames in 10 s");
    CHECK(sent >= 9);
}

TEST(engine_change_detection)
{
    en::FrameGate gate(1000);
    CHECK(gate.offer("a", 0));
    CHECK(!gate.offer("a", 5000)); // unchanged: never sent again, however old the last send
    CHECK(1 == gate.suppressedUnchanged());
    CHECK(gate.offer("b", 5100).value_or("") == "b"); // changed and the cap allows: sent
    CHECK(!gate.offer("c", 5200));          // held
    CHECK(!gate.offer("b", 5300));          // back to what the clients have: nothing to send
    CHECK(!gate.poll(7000));
    CHECK(gate.offer("d", 7100).value_or("") == "d");
}

// The engine frame reaches every connection on open and on change (a queued older one replaced); election frames
// reach every connection, never coalesced.
TEST(engine_frames_reach_every_connection)
{
    Rig rig;
    rig.start();
    rig.core.onEngineStatus(std::make_shared<const std::string>("{\"type\":\"engine\",\"v\":1}"));
    const std::size_t a = rig.connect(5, st::channel::ORDERS);
    rig.step(2);
    CHECK(1 == rig.clients[a].count("engine"));
    // a paused writer: two new status frames queue as one (the newest), two election frames as two
    rig.clients[a].out->capacity = 0;
    const std::size_t b = rig.connect(6, 0);
    rig.step(1);
    CHECK(1 == rig.clients[b].count("engine"));
    rig.core.onEngineStatus(std::make_shared<const std::string>("{\"type\":\"engine\",\"v\":2}"));
    rig.core.onEngineEvent(std::make_shared<const std::string>("{\"type\":\"election\",\"phase\":\"start\"}"));
    rig.core.onEngineStatus(std::make_shared<const std::string>("{\"type\":\"engine\",\"v\":3}"));
    rig.core.onEngineEvent(std::make_shared<const std::string>("{\"type\":\"election\",\"phase\":\"end\"}"));
    rig.step(1);
    rig.clients[a].out->capacity = 1 << 20;
    rig.step(3);
    CHECK(2 == rig.clients[a].count("engine"));
    CHECK(2 == rig.clients[a].count("election"));
    const std::string lastEngine = [&] {
        std::string s;
        for (const std::string &f : rig.clients[a].out->frames)
        {
            if (std::string::npos != f.find("\"engine\""))
            {
                s = f;
            }
        }
        return s;
    }();
    CHECK(std::string::npos != lastEngine.find("\"v\":3"));
    CHECK(3 == rig.clients[b].count("engine")); // b read every one as it came
    CHECK(2 == rig.clients[b].count("election"));
}

namespace {

// A loopback node endpoint: answers /health and /metrics with fixed bodies, or accepts and never answers.
class FakeNode
{
public:
    FakeNode(std::string healthBody, std::string metricsBody, bool mute = false) :
        m_acceptor(m_ioc, boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0)),
        m_health(std::move(healthBody)), m_metrics(std::move(metricsBody)), m_mute(mute)
    {
        m_thread = std::thread([this] { serve(); });
    }
    ~FakeNode()
    {
        m_stop = true;
        // a blocking accept is not woken by closing the acceptor from another thread: connect once to wake it
        boost::system::error_code ec;
        {
            boost::asio::io_context ioc;
            boost::asio::ip::tcp::socket wake(ioc);
            wake.connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), port()), ec);
            m_thread.join();
        }
        m_acceptor.close(ec);
    }
    std::uint16_t port() const { return m_acceptor.local_endpoint().port(); }
    std::atomic<int> requests{ 0 };

private:
    void serve()
    {
        while (!m_stop)
        {
            boost::asio::ip::tcp::socket s(m_ioc);
            boost::system::error_code ec;
            m_acceptor.accept(s, ec);
            if (ec || m_stop)
            {
                return;
            }
            requests++;
            if (m_mute)
            {
                m_held.push_back(std::move(s)); // accepted, never answered
                continue;
            }
            boost::asio::streambuf buf;
            boost::asio::read_until(s, buf, "\r\n\r\n", ec);
            const std::string head(boost::asio::buffers_begin(buf.data()), boost::asio::buffers_end(buf.data()));
            const bool metrics = std::string::npos != head.find("GET /metrics");
            const std::string &body = metrics ? m_metrics : m_health;
            const std::string reply = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: " +
                std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
            boost::asio::write(s, boost::asio::buffer(reply), ec);
            s.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
        }
    }
    boost::asio::io_context m_ioc;
    boost::asio::ip::tcp::acceptor m_acceptor;
    std::string m_health;
    std::string m_metrics;
    bool m_mute;
    std::atomic<bool> m_stop{ false };
    std::vector<boost::asio::ip::tcp::socket> m_held;
    std::thread m_thread;
};

std::uint16_t closedPort()
{
    boost::asio::io_context ioc;
    boost::asio::ip::tcp::acceptor a(ioc, boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    return a.local_endpoint().port(); // closed when this returns: nothing listens there
}

}

// The poller over real sockets: one node answers, one port refuses, one node accepts and never answers (bounded by the
// timeout). Each node's row comes from its own requests.
TEST(engine_poller_reads_each_node_itself)
{
    FakeNode leader(health("LEADER", 3, 4096, 4096), coreMetrics(0, 2048, 3));
    FakeNode mute("", "", true);
    st::EnginePollerSettings s;
    s.coreNodes = { { 0, "127.0.0.1", leader.port() }, { 1, "127.0.0.1", closedPort() }, { 2, "127.0.0.1", mute.port() } };
    s.timeoutMs = 300;
    s.frameMinMs = 1;
    st::EngineRing ring(st::ENGINE_RING_CELLS);
    st::EngineStats stats;
    st::EnginePoller poller(s, ring, stats);
    const auto t0 = std::chrono::steady_clock::now();
    poller.roundOnce(10'000, 10'000);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    CHECK_MSG(ms < 2000, "a round took " + std::to_string(ms) + " ms with a 300 ms request timeout");
    CHECK(6 == stats.requests.load());
    CHECK(4 == stats.requestFailures.load());
    CHECK(2 == leader.requests.load());
    st::EngineOut out;
    std::string frame;
    while (ring.tryPop(out))
    {
        if (st::EngineOut::Kind::STATUS == out.kind)
        {
            frame = *out.text;
        }
    }
    CHECK(!frame.empty());
    if (!frame.empty())
    {
        const json::object f = parsed(frame);
        const json::array &rows = f.at("core").at("nodes").as_array();
        CHECK(3 == rows.size());
        if (3 == rows.size())
        {
            CHECK("leader" == rows[0].at("role").as_string());
            CHECK(4096 == rows[0].at("commitPosition").as_int64());
            CHECK(2048 == rows[0].at("snapshot").at("position").as_int64());
            CHECK("unreachable" == rows[1].at("role").as_string());
            CHECK("unreachable" == rows[2].at("role").as_string());
            CHECK(std::string::npos != std::string(rows[2].at("error").as_string()).find("within 300 ms"));
        }
        CHECK(static_cast<std::int64_t>(frame.size()) == stats.lastFrameBytes.load());
    }
    // the same answers again: no new frame
    poller.roundOnce(11'000, 11'000);
    poller.roundOnce(12'000, 12'000);
    int statusFrames = 0;
    while (ring.tryPop(out))
    {
        statusFrames += st::EngineOut::Kind::STATUS == out.kind ? 1 : 0;
    }
    CHECK(0 == statusFrames);
    CHECK(2 == stats.unchanged.load());
}

// A body above STREAM_ENGINE_MAX_BODY_BYTES is no answer (the node is not trusted with a half-read body).
TEST(engine_poller_body_bound)
{
    FakeNode big(health("LEADER", 3, 1, 1), coreMetrics(0) + std::string(5000, '#') + "\n");
    std::vector<std::pair<en::NodeEndpointSpec, std::string>> req = { { { 0, "127.0.0.1", big.port() }, "/metrics" },
        { { 0, "127.0.0.1", big.port() }, "/health" } };
    const auto answers = st::httpGetAll(req, 500, 1024);
    CHECK(2 == answers.size());
    CHECK(!answers[0].answered && !answers[0].error.empty());
    CHECK(answers[1].answered && 200 == answers[1].status);
}

// A round is bounded by STREAM_ENGINE_POLL_TIMEOUT_MS even when a node's name lookup hangs (getaddrinfo cannot be
// cancelled once it runs), and stop() joins st-engine, never detaches a thread that uses the poller.
TEST(engine_poller_slow_dns_is_bounded)
{
    if (nullptr == dlsym(RTLD_DEFAULT, "gw_test_slow_dns_active"))
    {
        CHECK_MSG(false, "the slow name lookup shim is not preloaded (LD_PRELOAD=libgw_test_slow_dns.so)");
        return;
    }
    st::EnginePollerSettings s;
    s.coreNodes = { { 0, "node0.slow.invalid", 9700 }, { 1, "node1.slow.invalid", 9701 } };
    s.pollMs = 1000;
    s.timeoutMs = 200;
    s.frameMinMs = 1;
    st::EngineRing ring(st::ENGINE_RING_CELLS);
    st::EngineStats stats;
    auto poller = std::make_unique<st::EnginePoller>(s, ring, stats);
    const auto t0 = std::chrono::steady_clock::now();
    poller->roundOnce(10'000, 10'000);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    CHECK_MSG(ms < 600, "round not bounded by the timeout: " + std::to_string(ms) + " ms with a 200 ms timeout");
    CHECK(4 == stats.requestFailures.load());
    st::EngineOut out;
    std::string frame;
    while (ring.tryPop(out))
    {
        if (st::EngineOut::Kind::STATUS == out.kind)
        {
            frame = *out.text;
        }
    }
    CHECK(!frame.empty());
    if (!frame.empty())
    {
        const json::object f = parsed(frame);
        for (const json::value &r : f.at("core").at("nodes").as_array())
        {
            CHECK("unreachable" == r.at("role").as_string());
            CHECK(r.at("term").is_null());
        }
    }
    poller->start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto t1 = std::chrono::steady_clock::now();
    const bool joined = poller->stop(300);
    const auto stopMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t1).count();
    CHECK_MSG(joined, "st-engine detached at stop after " + std::to_string(stopMs) + " ms");
    CHECK_MSG(stopMs < 900, "stop took " + std::to_string(stopMs) + " ms");
    poller.reset(); // a detached thread still using the poller would now read freed memory
}

// A frame held by the rate cap goes out when the cap allows it, not at the next poll round.
TEST(engine_poller_releases_a_held_frame_at_the_cap)
{
    st::EnginePollerSettings s;
    s.coreNodes = { { 0, "127.0.0.1", closedPort() } };
    s.pollMs = 4000;
    s.timeoutMs = 300;
    s.frameMinMs = 400;
    st::EngineRing ring(st::ENGINE_RING_CELLS);
    st::EngineStats stats;
    st::EnginePoller poller(s, ring, stats);
    const auto t0 = std::chrono::steady_clock::now();
    poller.start(); // the all-null boot frame takes the cap slot; the first round's frame (unreachable) is held
    std::vector<std::int64_t> at;
    while (at.size() < 2 && std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(3000))
    {
        st::EngineOut out;
        if (ring.tryPop(out) && st::EngineOut::Kind::STATUS == out.kind)
        {
            at.push_back(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    poller.stop(1000);
    CHECK(!at.empty());
    CHECK_MSG(2 == at.size() && at[1] >= 350 && at[1] < 1200,
        "held engine frame waited for the next poll round (" +
            (2 == at.size() ? std::to_string(at[1]) + " ms" : std::string("not sent within 3000 ms")) + ", cap 400 ms)");
}
