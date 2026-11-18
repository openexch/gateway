#include "HistoryWork.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

namespace openexchange { namespace gateway { namespace projector {

using pg::Exec;
using pg::Result;

namespace {

// nullopt -> "" (each nullable parameter is written NULLIF($n, '') in the statement)
std::vector<std::string> params(const Columns &columns)
{
    std::vector<std::string> out;
    out.reserve(columns.size());
    for (const auto &c : columns)
    {
        out.push_back(c ? *c : std::string());
    }
    return out;
}

bool run(PgConn &conn, std::int64_t timeoutMs, const std::string &sql, const std::vector<std::string> &p,
    std::string &error, const char *what)
{
    Result r;
    if (Exec::OK != conn.exec(sql, p, timeoutMs, r, error, false))
    {
        error = std::string(what) + ": " + error;
        return false;
    }
    return true;
}

std::vector<std::uint8_t> fromHex(const std::string &text)
{
    std::vector<std::uint8_t> out;
    std::size_t i = 0 == text.rfind("\\x", 0) ? 2 : 0;
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') { return c - '0'; }
        if (c >= 'a' && c <= 'f') { return c - 'a' + 10; }
        if (c >= 'A' && c <= 'F') { return c - 'A' + 10; }
        return 0;
    };
    for (; i + 1 < text.size(); i += 2)
    {
        out.push_back(static_cast<std::uint8_t>(nibble(text[i]) * 16 + nibble(text[i + 1])));
    }
    return out;
}

std::string shaOfStored(const std::string &hexText)
{
    const std::vector<std::uint8_t> bytes = fromHex(hexText);
    return sha256Hex(bytes.data(), bytes.size());
}

const char *EVENT_INSERT =
    "INSERT INTO oe_order_events (journal_seq, order_id, user_id, event, status, core_log_position, liquidity, me_epoch, "
    "me_journal_seq, me_log_position, me_timestamp_ms, trade_id, last_price, last_qty, hold_remaining, at_ms, payload) "
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17::bytea)";

const char *ORDER_UPSERT =
    "INSERT INTO oe_orders (order_id, user_id, client_order_id, market_id, side, type, time_in_force, price, stop_price, "
    "quantity, filled_quantity, remaining_quantity, status, reason, me_reason, me_order_id, hold_asset_id, hold_amount, "
    "hold_remaining, created_at_ms, updated_at_ms, last_event_seq, last_log_position) "
    "VALUES ($1, $2, NULLIF($3, '')::uuid, $4, $5, $6, $7, NULLIF($8, '')::bigint, NULLIF($9, '')::bigint, $10, $11, $12, "
    "$13, NULLIF($14, ''), NULLIF($15, '')::smallint, NULLIF($16, '')::bigint, $17, $18, $19, $20, $21, $22, $23) "
    "ON CONFLICT (order_id) DO UPDATE SET user_id = EXCLUDED.user_id, client_order_id = EXCLUDED.client_order_id, "
    "market_id = EXCLUDED.market_id, side = EXCLUDED.side, type = EXCLUDED.type, time_in_force = EXCLUDED.time_in_force, "
    "price = EXCLUDED.price, stop_price = EXCLUDED.stop_price, quantity = EXCLUDED.quantity, "
    "filled_quantity = EXCLUDED.filled_quantity, remaining_quantity = EXCLUDED.remaining_quantity, "
    "status = EXCLUDED.status, reason = EXCLUDED.reason, me_reason = EXCLUDED.me_reason, "
    "me_order_id = EXCLUDED.me_order_id, hold_asset_id = EXCLUDED.hold_asset_id, hold_amount = EXCLUDED.hold_amount, "
    "hold_remaining = EXCLUDED.hold_remaining, created_at_ms = EXCLUDED.created_at_ms, "
    "updated_at_ms = EXCLUDED.updated_at_ms, last_event_seq = EXCLUDED.last_event_seq, "
    "last_log_position = EXCLUDED.last_log_position WHERE oe_orders.last_event_seq < EXCLUDED.last_event_seq";

const char *FILL_INSERT =
    "INSERT INTO oe_fills (trade_id, order_id, user_id, market_id, side, price, quantity, maker, executed_at_ms, "
    "event_seq, core_log_position, me_epoch, me_journal_seq, me_log_position, me_timestamp_ms) "
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::boolean, $9, $10, $11, $12, $13, $14, $15)";

}

namespace store {

bool loadMemo(PgConn &conn, std::int64_t timeoutMs, std::vector<MemoRow> &out, std::string &error)
{
    Result r;
    if (Exec::OK != conn.exec("SELECT source, archive_id, recording_id, start_timestamp, initial_term_id, session_id, "
            "term_buffer_length, mtu_length, stream_id, end_position, seq, epoch FROM oe_projection_source_memo "
            "ORDER BY source", {}, timeoutMs, r, error, false))
    {
        error = "memo rows: " + error;
        return false;
    }
    out.clear();
    for (int i = 0; i < PQntuples(r.get()); ++i)
    {
        auto v = [&](int c) { return std::string(PQgetvalue(r.get(), i, c)); };
        MemoRow m;
        m.source = v(0);
        m.key.archiveId = std::stoll(v(1));
        m.key.recordingId = std::stoll(v(2));
        m.key.startTimestamp = std::stoll(v(3));
        m.key.initialTermId = std::stoi(v(4));
        m.key.sessionId = std::stoi(v(5));
        m.key.termBufferLength = std::stoi(v(6));
        m.key.mtuLength = std::stoi(v(7));
        m.key.streamId = std::stoi(v(8));
        m.endPosition = std::stoll(v(9));
        m.seq = std::stoll(v(10));
        m.epoch = std::stoll(v(11));
        out.push_back(m);
    }
    return true;
}

bool loadReplays(PgConn &conn, std::int64_t timeoutMs, std::vector<ReplayRow> &out, std::string &error)
{
    Result r;
    if (Exec::OK != conn.exec("SELECT source, archive_id, replay_session_id FROM oe_projection_replay ORDER BY started_at, "
            "source, replay_session_id", {}, timeoutMs, r, error, false))
    {
        error = "replay rows: " + error;
        return false;
    }
    out.clear();
    for (int i = 0; i < PQntuples(r.get()); ++i)
    {
        ReplayRow row;
        row.source = PQgetvalue(r.get(), i, 0);
        row.archiveId = std::stoll(PQgetvalue(r.get(), i, 1));
        row.replaySessionId = std::stoll(PQgetvalue(r.get(), i, 2));
        out.push_back(row);
    }
    return true;
}

bool insertReplay(PgConn &conn, std::int64_t timeoutMs, const ReplayRow &row, std::string &error)
{
    return run(conn, timeoutMs, "INSERT INTO oe_projection_replay (source, archive_id, replay_session_id) VALUES ($1, $2, "
        "$3) ON CONFLICT (source, replay_session_id) DO NOTHING",
        { row.source, std::to_string(row.archiveId), std::to_string(row.replaySessionId) }, error, "replay row");
}

bool deleteReplay(PgConn &conn, std::int64_t timeoutMs, const ReplayRow &row, std::string &error)
{
    return run(conn, timeoutMs, "DELETE FROM oe_projection_replay WHERE source = $1 AND replay_session_id = $2",
        { row.source, std::to_string(row.replaySessionId) }, error, "replay row");
}

bool storedPayloads(PgConn &conn, std::int64_t timeoutMs, const std::vector<std::int64_t> &seqs,
    std::map<std::int64_t, std::string> &out, std::string &error)
{
    if (seqs.empty())
    {
        return true;
    }
    std::string list = "{";
    for (std::size_t i = 0; i < seqs.size(); ++i)
    {
        list += (i ? "," : "") + std::to_string(seqs[i]);
    }
    list += "}";
    Result r;
    if (Exec::OK != conn.exec("SELECT journal_seq, payload FROM oe_order_events WHERE journal_seq = ANY($1::bigint[])",
            { list }, timeoutMs, r, error, false))
    {
        error = "stored payloads: " + error;
        return false;
    }
    for (int i = 0; i < PQntuples(r.get()); ++i)
    {
        out[std::stoll(PQgetvalue(r.get(), i, 0))] = PQgetvalue(r.get(), i, 1);
    }
    return true;
}

bool applyRecord(PgConn &conn, std::int64_t timeoutMs, const JournalRecord &record, std::string &error)
{
    if (!run(conn, timeoutMs, EVENT_INSERT, params(eventColumns(record)), error, "oe_order_events"))
    {
        return false;
    }
#ifdef PROJ_NC_FILLS_FIRST
    // test build only: the fill is written before its order
    if (hasFill(record) && !run(conn, timeoutMs, FILL_INSERT, params(fillColumns(record)), error, "oe_fills"))
    {
        return false;
    }
    return run(conn, timeoutMs, ORDER_UPSERT, params(orderColumns(record)), error, "oe_orders");
#else
    if (!run(conn, timeoutMs, ORDER_UPSERT, params(orderColumns(record)), error, "oe_orders"))
    {
        return false;
    }
    return !hasFill(record) || run(conn, timeoutMs, FILL_INSERT, params(fillColumns(record)), error, "oe_fills");
#endif
}

bool upsertMemo(PgConn &conn, std::int64_t timeoutMs, const MemoRow &row, std::string &error)
{
    const ar::RecordingKey &k = row.key;
    return run(conn, timeoutMs,
        "INSERT INTO oe_projection_source_memo (source, archive_id, recording_id, start_timestamp, initial_term_id, "
        "session_id, term_buffer_length, mtu_length, stream_id, end_position, seq, epoch) VALUES ($1, $2, $3, $4, $5, $6, "
        "$7, $8, $9, $10, $11, $12) ON CONFLICT (source) DO UPDATE SET archive_id = EXCLUDED.archive_id, "
        "recording_id = EXCLUDED.recording_id, start_timestamp = EXCLUDED.start_timestamp, "
        "initial_term_id = EXCLUDED.initial_term_id, session_id = EXCLUDED.session_id, "
        "term_buffer_length = EXCLUDED.term_buffer_length, mtu_length = EXCLUDED.mtu_length, "
        "stream_id = EXCLUDED.stream_id, end_position = EXCLUDED.end_position, seq = EXCLUDED.seq, "
        "epoch = EXCLUDED.epoch, updated_at = clock_timestamp()",
        { row.source, std::to_string(k.archiveId), std::to_string(k.recordingId), std::to_string(k.startTimestamp),
            std::to_string(k.initialTermId), std::to_string(k.sessionId), std::to_string(k.termBufferLength),
            std::to_string(k.mtuLength), std::to_string(k.streamId), std::to_string(row.endPosition),
            std::to_string(row.seq), std::to_string(row.epoch) },
        error, "memo row");
}

bool writeHalt(PgConn &conn, std::int64_t timeoutMs, const HaltState &halt, std::string &error)
{
    return run(conn, timeoutMs, "UPDATE oe_projection_state SET halt_reason = NULLIF($1, ''), halt_detail = NULLIF($2, "
        "'') WHERE singleton", { Halt::NONE == halt.halt ? std::string() : std::string(nameOf(halt.halt)),
                                   Halt::NONE == halt.halt ? std::string() : halt.detail },
        error, "halt");
}

}

void HistoryWork::onWriter(const ProjectionState &state)
{
    if (m_loaded && !m_needLoad)
    {
        // read again after a failed statement or a lost connection: the cursor decides what the last batch did
        if (state.appliedSeq == m_state.appliedSeq && state.epoch == m_state.epoch)
        {
            m_stats.uncertainCommits.fetch_add(m_batch.empty() ? 0 : 1, std::memory_order_relaxed);
            return; // it did not commit: m_batch is written again
        }
        if (0 != m_batchLastSeq && state.appliedSeq == m_batchLastSeq)
        {
            m_stats.uncertainCommits.fetch_add(1, std::memory_order_relaxed);
            // it committed: the outcome computed for it holds
            StateAdvance advance;
            advance.appliedSeq = state.appliedSeq;
            advance.epoch = state.epoch;
            advance.appliedLogPosition = state.appliedLogPosition;
            advance.appliedOrderId = state.appliedOrderId;
            committed(advance);
            return;
        }
    }
    // a takeover, or a cursor that is neither: start a generation from the database
    m_state = state;
    m_needLoad = true;
    m_batch.clear();
    m_batchLastSeq = 0;
    m_outcome = Outcome{};
}

void HistoryWork::onStandby()
{
    m_exchange.standby();
    m_loaded = false;
    m_needLoad = false;
    m_batch.clear();
    m_batchLastSeq = 0;
    m_outcome = Outcome{};
}

bool HistoryWork::due(std::int64_t nowNs)
{
    if (m_needLoad)
    {
        return true;
    }
    if (!m_loaded)
    {
        return false;
    }
    std::size_t records = 0;
    bool urgent = false;
    for (const Item &i : m_batch)
    {
        records += Item::Kind::APPLY == i.kind || Item::Kind::COMPARE == i.kind ? 1 : 0;
        urgent = urgent || !(Item::Kind::APPLY == i.kind || Item::Kind::COMPARE == i.kind);
    }
    const std::size_t room = static_cast<std::size_t>(m_settings.batchEvents) > records
        ? static_cast<std::size_t>(m_settings.batchEvents) - records : 0;
    if (room > 0)
    {
        std::vector<Item> more = m_exchange.take(room);
        if (!more.empty() && m_batch.empty())
        {
            m_batchStartNs = nowNs;
        }
        for (Item &i : more)
        {
            const bool record = Item::Kind::APPLY == i.kind || Item::Kind::COMPARE == i.kind;
            records += record ? 1 : 0;
            urgent = urgent || !record;
            m_batch.push_back(std::move(i));
        }
    }
    if (m_batch.empty())
    {
        return false;
    }
    return urgent || records >= static_cast<std::size_t>(m_settings.batchEvents) ||
        nowNs - m_batchStartNs >= m_settings.batchMs * 1'000'000LL;
}

bool HistoryWork::write(PgConn &conn, const std::string &, StateAdvance &advance, std::string &error)
{
    const std::int64_t t = m_settings.statementTimeoutMs;
    m_outcome = Outcome{};
    if (m_needLoad)
    {
        return store::loadMemo(conn, t, m_loadMemo, error) && store::loadReplays(conn, t, m_loadReplays, error);
    }
    // the stored bytes of every compared sequence at or below the committed cursor, in one statement
    std::vector<std::int64_t> storedSeqs;
    for (const Item &i : m_batch)
    {
        if (Item::Kind::COMPARE == i.kind && i.record.seq <= m_state.appliedSeq)
        {
            storedSeqs.push_back(i.record.seq);
        }
    }
    std::map<std::int64_t, std::string> stored;
    if (!store::storedPayloads(conn, t, storedSeqs, stored, error))
    {
        return false;
    }
#ifdef PROJ_NC_CURSOR_OWN_TX
    // test build only: the cursor commits in a transaction of its own, before the rows
    {
        std::int64_t last = 0;
        std::int64_t lastEpoch = 0;
        for (const Item &i : m_batch)
        {
            last = Item::Kind::APPLY == i.kind ? i.record.seq : last;
            lastEpoch = Item::Kind::APPLY == i.kind ? i.record.epoch : lastEpoch;
        }
        if (0 != last && !(run(conn, t, "UPDATE oe_projection_state SET applied_seq = $1, epoch = coalesce(epoch, $2) "
                "WHERE singleton", { std::to_string(last), std::to_string(lastEpoch) }, error, "cursor") &&
                run(conn, t, "COMMIT", {}, error, "cursor") &&
                run(conn, t, "BEGIN", {}, error, "cursor")))
        {
            return false;
        }
    }
#endif
    std::map<std::int64_t, const Item *> appliedHere; // seq -> the APPLY item of this batch
    std::map<std::string, MemoRow> memo;              // per source: the last record applied from it
    std::int64_t orderIdMax = m_state.appliedOrderId;
    std::optional<std::int64_t> epoch;
    std::int64_t expect = m_state.appliedSeq + 1; // the only sequence an APPLY may carry next
    for (const Item &i : m_batch)
    {
        switch (i.kind)
        {
            case Item::Kind::APPLY:
            {
#ifdef PROJ_TC_SOURCE_FAULTS
                // test build only: PROJ_TC_RELOAD_AT=<seq> fails the batch that applies that sequence, once, the way
                // a belt below does (a new generation from the database while the follow side is attached)
                {
                    static const std::int64_t reloadAt =
                        nullptr == std::getenv("PROJ_TC_RELOAD_AT") ? 0 : std::atoll(std::getenv("PROJ_TC_RELOAD_AT"));
                    static bool reloaded = false;
                    if (!reloaded && reloadAt > 0 && i.record.seq == reloadAt)
                    {
                        reloaded = true;
                        error = "test fault: the batch applying journalSeq " + std::to_string(reloadAt) +
                            " fails once and a generation starts from the database";
                        m_needLoad = true;
                        return false;
                    }
                }
#endif
#if !defined(PROJ_NC_NO_EPOCH_CHECK) && !defined(PROJ_NC_EPOCH_FROM_FIRST_RECORD)
                // a record of another genesis is never written: the batch fails and the generation starts again
                // from the database (pj-duty filters such records before they are queued; this is the last stop)
                if (const std::optional<std::int64_t> e = epoch ? epoch : m_state.epoch; !e || i.record.epoch != *e)
                {
                    error = "record of epoch " + std::to_string(i.record.epoch) + " at journalSeq " +
                        std::to_string(i.record.seq) + " in a projection of epoch " +
                        (e ? std::to_string(*e) : std::string("none")) + ": refused, nothing of this batch is written";
                    m_needLoad = true;
                    return false;
                }
#endif
#ifndef PROJ_NC_NO_DENSITY_CHECK
                // the rows stay a dense prefix whatever pj-duty queued: an APPLY that is not the record after the
                // cursor (a gap, or one applied twice) fails the batch and a generation starts from the database
                if (i.record.seq != expect)
                {
                    error = "APPLY of journalSeq " + std::to_string(i.record.seq) + " is not the next record (" +
                        std::to_string(expect) + " is): refused, nothing of this batch is written";
                    m_needLoad = true;
                    return false;
                }
#endif
                expect = i.record.seq + 1;
                if (!store::applyRecord(conn, t, i.record, error))
                {
                    return false;
                }
                appliedHere[i.record.seq] = &i;
                orderIdMax = std::max(orderIdMax, i.record.event.orderId);
                advance.appliedSeq = i.record.seq;
                advance.appliedLogPosition = i.record.logPosition;
                MemoRow m;
                m.source = i.sourceName;
                m.key = i.key;
                m.endPosition = i.endPosition;
                m.seq = i.record.seq;
                m.epoch = i.record.epoch;
                memo[i.sourceName] = m;
                CommittedProof p;
                p.source = i.source;
                p.key = i.key;
                p.seq = i.record.seq;
                p.epoch = i.record.epoch;
                p.endPosition = i.endPosition;
                m_outcome.proofs.push_back(p);
                if (m_outcome.appliedFrom.end() ==
                    std::find(m_outcome.appliedFrom.begin(), m_outcome.appliedFrom.end(), i.source))
                {
                    m_outcome.appliedFrom.push_back(i.source);
                }
                m_outcome.applied++;
                break;
            }
            case Item::Kind::COMPARE:
            {
                const std::string mine = "\\x" + hexOf(i.record.payload.data(), i.record.payload.size());
                std::optional<std::string> theirs;
                if (i.record.seq <= m_state.appliedSeq)
                {
                    if (auto it = stored.find(i.record.seq); it != stored.end())
                    {
                        theirs = it->second;
                    }
                }
                else if (auto it = appliedHere.find(i.record.seq); it != appliedHere.end())
                {
                    theirs = "\\x" + hexOf(it->second->record.payload.data(), it->second->record.payload.size());
                }
                m_outcome.compared++;
#ifdef PROJ_NC_NO_COMPARE
                (void)mine; // test build only: a duplicate is skipped without the comparison
                m_outcome.equal++;
#else
                if (theirs && *theirs == mine)
                {
                    m_outcome.equal++;
                    if (m_outcome.comparedFrom.end() ==
                        std::find(m_outcome.comparedFrom.begin(), m_outcome.comparedFrom.end(), i.source))
                    {
                        m_outcome.comparedFrom.push_back(i.source);
                        m_outcome.comparedUpTo.emplace_back(i.source, i.record.seq);
                    }
                    for (auto &[src, upTo] : m_outcome.comparedUpTo)
                    {
                        upTo = src == i.source ? std::max(upTo, i.record.seq) : upTo;
                    }
                }
                else
                {
                    Conflict c;
                    c.source = i.source;
                    c.sourceName = i.sourceName;
                    c.seq = i.record.seq;
                    c.storedSha256 = theirs ? shaOfStored(*theirs) : std::string("none stored");
                    c.readSha256 = sha256Hex(i.record.payload.data(), i.record.payload.size());
                    m_outcome.conflicts.push_back(c);
                }
#endif
                break;
            }
            case Item::Kind::REPLAY_STARTED:
                if (!store::insertReplay(conn, t, i.replay, error))
                {
                    return false;
                }
                m_outcome.replaysRecorded.push_back(i.replay.replaySessionId);
                break;
            case Item::Kind::REPLAY_ENDED:
                if (!store::deleteReplay(conn, t, i.replay, error))
                {
                    return false;
                }
                break;
            case Item::Kind::EPOCH:
                if (!m_state.epoch)
                {
                    epoch = i.epoch;
                }
                break;
            case Item::Kind::HALT:
                if (!store::writeHalt(conn, t, i.halt, error))
                {
                    return false;
                }
                m_outcome.halt = i.halt;
                break;
        }
    }
    for (const auto &[source, row] : memo)
    {
        if (!store::upsertMemo(conn, t, row, error))
        {
            return false;
        }
    }
    if (orderIdMax != m_state.appliedOrderId)
    {
        advance.appliedOrderId = orderIdMax;
    }
    if (epoch)
    {
        advance.epoch = epoch;
    }
    m_batchLastSeq = advance.appliedSeq.value_or(0);
    return true;
}

void HistoryWork::committed(const StateAdvance &advance)
{
    if (m_needLoad)
    {
        m_needLoad = false;
        m_loaded = true;
        Reset r;
        r.state = m_state;
        r.memo = m_loadMemo;
        r.replays = m_loadReplays;
        m_stats.resets.fetch_add(1, std::memory_order_relaxed);
        m_exchange.reset(std::move(r));
        m_batch.clear();
        m_batchLastSeq = 0;
        return;
    }
    if (advance.epoch) { m_state.epoch = advance.epoch; }
    if (advance.appliedSeq) { m_state.appliedSeq = *advance.appliedSeq; }
    if (advance.appliedLogPosition) { m_state.appliedLogPosition = *advance.appliedLogPosition; }
    if (advance.appliedOrderId) { m_state.appliedOrderId = *advance.appliedOrderId; }
    if (m_outcome.halt)
    {
        m_state.haltReason = Halt::NONE == m_outcome.halt->halt ? std::nullopt
                                                                : std::optional<std::string>(nameOf(m_outcome.halt->halt));
    }
    m_stats.recordsApplied.fetch_add(m_outcome.applied, std::memory_order_relaxed);
    m_stats.skips.fetch_add(m_outcome.equal, std::memory_order_relaxed);
    m_stats.payloadConflicts.fetch_add(m_outcome.conflicts.size(), std::memory_order_relaxed);
    m_stats.replayRows.fetch_add(m_outcome.replaysRecorded.size(), std::memory_order_relaxed);
    for (const Conflict &c : m_outcome.conflicts)
    {
        std::fprintf(stderr, "oe-projector: CRITICAL payload conflict at journalSeq %lld from source %s: stored sha256 %s, "
            "read sha256 %s; nothing further is applied from that source\n", static_cast<long long>(c.seq),
            c.sourceName.c_str(), c.storedSha256.c_str(), c.readSha256.c_str());
    }
    const std::int64_t applied = m_state.appliedSeq;
    const std::optional<std::int64_t> epochNow = m_state.epoch;
    Outcome o = std::move(m_outcome);
    m_exchange.publish([&](Results &r) {
        r.appliedSeq = applied;
        r.epoch = epochNow;
        for (auto &p : o.proofs) { r.proofs.push_back(p); }
        r.comparesVerified += o.compared;
        r.comparesEqual += o.equal;
        for (auto &c : o.conflicts) { r.conflicts.push_back(c); }
        for (auto id : o.replaysRecorded) { r.replaysRecorded.push_back(id); }
        for (auto s : o.appliedFrom) { r.appliedFrom.push_back(s); }
        for (auto s : o.comparedFrom) { r.comparedFrom.push_back(s); }
        for (auto u : o.comparedUpTo) { r.comparedUpTo.push_back(u); }
    });
    m_outcome = Outcome{};
    m_batch.clear();
    m_batchLastSeq = 0;
}

}}}
