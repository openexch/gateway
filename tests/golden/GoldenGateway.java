// SPDX-License-Identifier: Apache-2.0
// Prints the golden wire frames of every money-schema template the gateway encodes, decodes or ignores, written by the
// core's OWN generated Java encoders, as C++ initialisers for tests/unit/WireGoldenTest.cpp (run by regen.sh with
// the core jar built at the commit dependencies.lock pins). Every field gets a value that is distinct per template,
// entry and field (v, i below; WireGoldenTest.cpp computes the same values), so a swapped or shifted field cannot
// pass.

import org.agrona.concurrent.UnsafeBuffer;

import com.openexchange.core.infrastructure.generated.*;

public final class GoldenGateway {

    // the values: template t, group entry e (0 outside a group), field k (schema field id)
    static long v(final int t, final int e, final int k) {
        return ((long) t << 40) | ((long) e << 32) | ((long) k << 24) | 0x5A5AL;
    }

    static int i(final int t, final int e, final int k) {
        return (t << 20) | (e << 12) | (k << 4) | 0xA;
    }

    private static final UnsafeBuffer BUF = new UnsafeBuffer(new byte[8192]);
    private static final MessageHeaderEncoder HEADER = new MessageHeaderEncoder();

    private static void print(final String name, final int bodyLength) {
        final int length = HEADER.encodedLength() + bodyLength;
        final StringBuilder sb = new StringBuilder();
        sb.append("static const std::vector<std::uint8_t> ").append(name).append(" = {");
        for (int n = 0; n < length; n++) {
            if (n % 16 == 0) {
                sb.append("\n    ");
            }
            sb.append(String.format("0x%02x,", BUF.getByte(n) & 0xFF));
            if (n % 16 != 15 && n != length - 1) {
                sb.append(' ');
            }
        }
        sb.append("\n};");
        System.out.println(sb);
    }

    // ---- commands the gateway encodes ----

    private static void commands() {
        final DepositEncoder deposit = new DepositEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(1, 0, 1)).userId(v(1, 0, 2)).assetId(i(1, 0, 3)).amount(v(1, 0, 4))
                .requestIdHigh(v(1, 0, 5)).requestIdLow(v(1, 0, 6));
        print("JAVA_DEPOSIT", deposit.encodedLength());

        final WithdrawEncoder withdraw = new WithdrawEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(2, 0, 1)).userId(v(2, 0, 2)).assetId(i(2, 0, 3)).amount(v(2, 0, 4))
                .requestIdHigh(v(2, 0, 5)).requestIdLow(v(2, 0, 6));
        print("JAVA_WITHDRAW", withdraw.encodedLength());

        final CreateOrderEncoder create = new CreateOrderEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(50, 0, 1)).userId(v(50, 0, 2)).clientOrderIdHigh(v(50, 0, 3))
                .clientOrderIdLow(v(50, 0, 4)).requestIdHigh(v(50, 0, 5)).requestIdLow(v(50, 0, 6))
                .marketId(i(50, 0, 7)).side(Side.SELL).orderType(OrderType.STOP_LIMIT).timeInForce(TimeInForce.GTD)
                .price(v(50, 0, 11)).quantity(v(50, 0, 12)).budget(v(50, 0, 13)).stopPrice(v(50, 0, 14))
                .trailingDelta(v(50, 0, 15)).displayQty(v(50, 0, 16)).expiresAtMs(v(50, 0, 17));
        create.flags().clear();
        print("JAVA_CREATE_ORDER", create.encodedLength());

        final CancelOrderEncoder cancel = new CancelOrderEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(51, 0, 1)).userId(v(51, 0, 2)).orderId(v(51, 0, 3)).requestIdHigh(v(51, 0, 4))
                .requestIdLow(v(51, 0, 5)).origClientOrderIdHigh(v(51, 0, 6)).origClientOrderIdLow(v(51, 0, 7));
        print("JAVA_CANCEL_ORDER", cancel.encodedLength());

        final AmendOrderEncoder amend = new AmendOrderEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(52, 0, 1)).userId(v(52, 0, 2)).orderId(v(52, 0, 3)).requestIdHigh(v(52, 0, 4))
                .requestIdLow(v(52, 0, 5)).newPrice(v(52, 0, 6)).newQuantity(v(52, 0, 7));
        print("JAVA_AMEND_ORDER", amend.encodedLength());

        final RequestOrderSnapshotEncoder snapshot = new RequestOrderSnapshotEncoder()
                .wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(53, 0, 1)).userId(v(53, 0, 2)).fromOrderId(v(53, 0, 3));
        print("JAVA_REQUEST_ORDER_SNAPSHOT", snapshot.encodedLength());

        final RequestOrderEventsEncoder events = new RequestOrderEventsEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(54, 0, 1)).fromEventSeq(v(54, 0, 2)).maxEvents(i(54, 0, 3));
        print("JAVA_REQUEST_ORDER_EVENTS", events.encodedLength());
        // the head probe: maxEvents 0
        final RequestOrderEventsEncoder probe = new RequestOrderEventsEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(54, 1, 1)).fromEventSeq(v(54, 1, 2)).maxEvents(0);
        print("JAVA_REQUEST_ORDER_EVENTS_HEAD_PROBE", probe.encodedLength());

        final RequestUserBalancesEncoder balances = new RequestUserBalancesEncoder()
                .wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(55, 0, 1)).userId(v(55, 0, 2));
        print("JAVA_REQUEST_USER_BALANCES", balances.encodedLength());

        final RequestCoreConfigEncoder config = new RequestCoreConfigEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(56, 0, 1));
        print("JAVA_REQUEST_CORE_CONFIG", config.encodedLength());

        // what the session registrar sends: class GATEWAY, resumable, identity = the instance id, round 1 of the
        // registrar's correlation base; oe-gateway registers acks|control, oe-stream acks|orders|control|balances
        final RegisterSessionEncoder gateway = new RegisterSessionEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(0x5E55_0000_0000L + 1).sessionClass(SessionClass.GATEWAY).readerKind((short) 0)
                .resumable(BoolFlag.TRUE).identity(v(80, 0, 6)).sourceEpoch(0L);
        gateway.channels().clear().acks(true).control(true);
        print("JAVA_REGISTER_SESSION_GATEWAY", gateway.encodedLength());
        final RegisterSessionEncoder stream = new RegisterSessionEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(0x5E55_0000_0000L + 1).sessionClass(SessionClass.GATEWAY).readerKind((short) 0)
                .resumable(BoolFlag.TRUE).identity(v(80, 1, 6)).sourceEpoch(0L);
        stream.channels().clear().acks(true).balances(true).orders(true).control(true);
        print("JAVA_REGISTER_SESSION_STREAM", stream.encodedLength());
        final RegisterSessionEncoder treasury = new RegisterSessionEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(0x5E55_0000_0000L + 1).sessionClass(SessionClass.TREASURY).readerKind((short) 0)
                .resumable(BoolFlag.TRUE).identity(v(80, 2, 6)).sourceEpoch(0L);
        treasury.channels().clear().acks(true);
        print("JAVA_REGISTER_SESSION_TREASURY", treasury.encodedLength());
    }

    // ---- replies and broadcasts the gateway decodes ----

    private static void event(final int t, final int e, final OrderEventEncoder enc) {
        enc.eventSeq(v(t, e, 1)).logPosition(v(t, e, 2)).orderId(v(t, e, 3)).userId(v(t, e, 4))
                .marketId(i(t, e, 5)).eventKind(EventKind.REPLACED).status(OrderStatus.PENDING_REPLACE)
                .reason(CoreRejectReason.ME_RELEASED).side(Side.SELL).orderType(OrderType.ICEBERG)
                .timeInForce(TimeInForce.FOK).price(v(t, e, 12)).stopPrice(v(t, e, 13)).quantity(v(t, e, 14))
                .filledQty(v(t, e, 15)).remainingQty(v(t, e, 16)).lastPrice(v(t, e, 17)).lastQty(v(t, e, 18))
                .tradeId(v(t, e, 19)).meOrderId(v(t, e, 20)).holdAssetId(i(t, e, 21)).holdAmount(v(t, e, 22))
                .holdRemaining(v(t, e, 23)).clientOrderIdHigh(v(t, e, 24)).clientOrderIdLow(v(t, e, 25))
                .createdAtMs(v(t, e, 26)).updatedAtMs(v(t, e, 27)).meReason((short) (200 + e));
    }

    private static void batchEvent(final int t, final int e, final OrderEventBatchEncoder.EventsEncoder enc) {
        enc.eventSeq(v(t, e, 1)).logPosition(v(t, e, 2)).orderId(v(t, e, 3)).userId(v(t, e, 4))
                .marketId(i(t, e, 5)).eventKind(EventKind.FILL).status(OrderStatus.PARTIALLY_FILLED)
                .reason(CoreRejectReason.NONE).side(Side.BUY).orderType(OrderType.LIMIT)
                .timeInForce(TimeInForce.GTC).price(v(t, e, 12)).stopPrice(v(t, e, 13)).quantity(v(t, e, 14))
                .filledQty(v(t, e, 15)).remainingQty(v(t, e, 16)).lastPrice(v(t, e, 17)).lastQty(v(t, e, 18))
                .tradeId(v(t, e, 19)).meOrderId(v(t, e, 20)).holdAssetId(i(t, e, 21)).holdAmount(v(t, e, 22))
                .holdRemaining(v(t, e, 23)).clientOrderIdHigh(v(t, e, 24)).clientOrderIdLow(v(t, e, 25))
                .createdAtMs(v(t, e, 26)).updatedAtMs(v(t, e, 27)).meReason((short) (200 + e));
    }

    private static void pageEvent(final int t, final int e, final OrderEventsPageEncoder.EventsEncoder enc) {
        enc.eventSeq(v(t, e, 1)).logPosition(v(t, e, 2)).orderId(v(t, e, 3)).userId(v(t, e, 4))
                .marketId(i(t, e, 5)).eventKind(EventKind.EXPIRED).status(OrderStatus.EXPIRED)
                .reason(CoreRejectReason.EXPIRED).side(Side.SELL).orderType(OrderType.TRAILING_STOP)
                .timeInForce(TimeInForce.GTD).price(v(t, e, 12)).stopPrice(v(t, e, 13)).quantity(v(t, e, 14))
                .filledQty(v(t, e, 15)).remainingQty(v(t, e, 16)).lastPrice(v(t, e, 17)).lastQty(v(t, e, 18))
                .tradeId(v(t, e, 19)).meOrderId(v(t, e, 20)).holdAssetId(i(t, e, 21)).holdAmount(v(t, e, 22))
                .holdRemaining(v(t, e, 23)).clientOrderIdHigh(v(t, e, 24)).clientOrderIdLow(v(t, e, 25))
                .createdAtMs(v(t, e, 26)).updatedAtMs(v(t, e, 27)).meReason((short) (200 + e));
    }

    private static void replies() {
        final AdminAckEncoder ack = new AdminAckEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(110, 0, 1)).refTemplateId(80).result(AdminResult.ACCEPTED).reason((short) 0)
                .configVersion(i(110, 0, 5));
        print("JAVA_ADMIN_ACK_REGISTER", ack.encodedLength());

        final AdmissionStateEncoder admission = new AdmissionStateEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .marketId(i(109, 0, 1)).gateState(GateState.SUSPENDED).gateReason(GateReason.RECOVERY)
                .circuitBreaker(BoolFlag.TRUE).atMs(v(109, 0, 5)).logPosition(v(109, 0, 6));
        admission.gateReasons().clear().recovery(true).meBehavior(true);
        print("JAVA_ADMISSION_STATE", admission.encodedLength());

        final ConfigAppliedEncoder applied = new ConfigAppliedEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .kind(ConfigKind.LIMITS).marketId(0).configVersion(i(112, 0, 3)).logPosition(v(112, 0, 4));
        print("JAVA_CONFIG_APPLIED", applied.encodedLength());

        final OrderAckEncoder orderAck = new OrderAckEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(100, 0, 1)).requestIdHigh(v(100, 0, 2)).requestIdLow(v(100, 0, 3))
                .orderId(v(100, 0, 4)).userId(v(100, 0, 5)).marketId(i(100, 0, 6)).status(OrderStatus.PENDING_NEW)
                .duplicate(BoolFlag.TRUE).holdAssetId(i(100, 0, 9)).holdAmount(v(100, 0, 10))
                .acceptedAtMs(v(100, 0, 11)).eventSeq(v(100, 0, 12));
        print("JAVA_ORDER_ACK", orderAck.encodedLength());

        final OrderRejectEncoder reject = new OrderRejectEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(101, 0, 1)).requestIdHigh(v(101, 0, 2)).requestIdLow(v(101, 0, 3))
                .userId(v(101, 0, 4)).orderId(v(101, 0, 5)).marketId(i(101, 0, 6)).refTemplateId(50)
                .reason(CoreRejectReason.REQUEST_ID_PAYLOAD_MISMATCH).meReason((short) 255).detail(v(101, 0, 10))
                .rejectedAtMs(v(101, 0, 11));
        print("JAVA_ORDER_REJECT", reject.encodedLength());

        final OrderEventEncoder orderEvent = new OrderEventEncoder().wrapAndApplyHeader(BUF, 0, HEADER);
        event(102, 0, orderEvent);
        print("JAVA_ORDER_EVENT", orderEvent.encodedLength());

        final OrderEventBatchEncoder batch = new OrderEventBatchEncoder().wrapAndApplyHeader(BUF, 0, HEADER);
        final OrderEventBatchEncoder.EventsEncoder batchEvents = batch.eventsCount(3);
        for (int e = 0; e < 3; e++) {
            batchEvent(103, e, batchEvents.next());
        }
        print("JAVA_ORDER_EVENT_BATCH", batch.encodedLength());

        final OrderEventBatchEncoder emptyBatch = new OrderEventBatchEncoder().wrapAndApplyHeader(BUF, 0, HEADER);
        emptyBatch.eventsCount(0);
        print("JAVA_ORDER_EVENT_BATCH_EMPTY", emptyBatch.encodedLength());

        final OrderSnapshotBatchEncoder snapshot = new OrderSnapshotBatchEncoder().wrapAndApplyHeader(BUF, 0, HEADER);
        final OrderSnapshotBatchEncoder.OrdersEncoder orders = snapshot.ordersCount(2);
        for (int e = 0; e < 2; e++) {
            final int t = 106;
            orders.next().orderId(v(t, e, 1)).userId(v(t, e, 2)).marketId(i(t, e, 3))
                    .status(OrderStatus.PENDING_TRIGGER).side(Side.BUY).orderType(OrderType.STOP_LOSS)
                    .timeInForce(TimeInForce.IOC).price(v(t, e, 8)).quantity(v(t, e, 9)).filledQty(v(t, e, 10))
                    .remainingQty(v(t, e, 11)).stopPrice(v(t, e, 12)).displayQty(v(t, e, 13))
                    .expiresAtMs(v(t, e, 14)).holdAssetId(i(t, e, 15)).holdAmount(v(t, e, 16))
                    .clientOrderIdHigh(v(t, e, 17)).clientOrderIdLow(v(t, e, 18)).meOrderId(v(t, e, 19))
                    .trailingArm(v(t, e, 20)).hiddenQty(v(t, e, 21)).sliceSeq(i(t, e, 22)).budget(v(t, e, 23))
                    .createdAtMs(v(t, e, 24)).updatedAtMs(v(t, e, 25));
        }
        print("JAVA_ORDER_SNAPSHOT_BATCH", snapshot.encodedLength());

        final OrderSnapshotEndEncoder snapshotEnd = new OrderSnapshotEndEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(107, 0, 1)).entryCount(i(107, 0, 2)).nextOrderId(v(107, 0, 3))
                .asOfLogPosition(v(107, 0, 4));
        print("JAVA_ORDER_SNAPSHOT_END", snapshotEnd.encodedLength());

        final OrderEventsPageEncoder page = new OrderEventsPageEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(115, 0, 1));
        final OrderEventsPageEncoder.EventsEncoder pageEvents = page.eventsCount(2);
        for (int e = 0; e < 2; e++) {
            pageEvent(115, e + 1, pageEvents.next());
        }
        print("JAVA_ORDER_EVENTS_PAGE", page.encodedLength());

        final OrderEventsEndEncoder eventsEnd = new OrderEventsEndEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(116, 0, 1)).count(i(116, 0, 2)).firstAvailableSeq(v(116, 0, 3))
                .lastSeq(v(116, 0, 4)).nextFromSeq(v(116, 0, 5));
        print("JAVA_ORDER_EVENTS_END", eventsEnd.encodedLength());

        final UserBalancesEncoder balances = new UserBalancesEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(117, 0, 1)).userId(v(117, 0, 2)).known(BoolFlag.TRUE)
                .asOfLogPosition(v(117, 0, 4));
        final UserBalancesEncoder.BalancesEncoder lines = balances.balancesCount(3);
        for (int e = 1; e <= 3; e++) {
            lines.next().assetId(i(117, e, 1)).available(v(117, e, 2)).locked(v(117, e, 3));
        }
        print("JAVA_USER_BALANCES", balances.encodedLength());

        final UserBalancesEncoder unknown = new UserBalancesEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(117, 9, 1)).userId(v(117, 9, 2)).known(BoolFlag.FALSE)
                .asOfLogPosition(v(117, 9, 4));
        unknown.balancesCount(0);
        print("JAVA_USER_BALANCES_UNKNOWN", unknown.encodedLength());

        final int t = 118;
        final CoreConfigReportEncoder report = new CoreConfigReportEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(t, 0, 1)).configVersion(i(t, 0, 2)).behaviorVersion(4).limitsVersion(i(t, 0, 4))
                .requestTtlMs(v(t, 0, 5)).requestPerUserCap(i(t, 0, 6)).orderEventRingCapacity(i(t, 0, 7))
                .gateState(GateState.SUSPENDED).asOfLogPosition(v(t, 0, 10));
        report.gateReasons().clear().admin(true).genesis(true);
        final CoreConfigReportEncoder.MarketsEncoder markets = report.marketsCount(2);
        final String[] symbols = { "BTC-USDT", "ETH-USDT-PERP-01" };
        for (int e = 1; e <= 2; e++) {
            final CoreConfigReportEncoder.MarketsEncoder m = markets.next().marketId(i(t, e, 1))
                    .symbol(symbols[e - 1]).baseAssetId(i(t, e, 3)).quoteAssetId(i(t, e, 4))
                    .tickSize(v(t, e, 5)).minPrice(v(t, e, 6)).maxPrice(v(t, e, 7)).lotSize(v(t, e, 8))
                    .minQuantity(v(t, e, 9)).maxQuantity(v(t, e, 10)).minNotional(v(t, e, 11))
                    .maxNotional(v(t, e, 12)).riskConfigVersion(i(t, e, 13))
                    .gateState(e == 1 ? GateState.OPEN : GateState.SUSPENDED)
                    .circuitBreaker(e == 1 ? BoolFlag.FALSE : BoolFlag.TRUE);
            if (e == 2) {
                m.gateReasons().clear().meCapacity(true);
            } else {
                m.gateReasons().clear();
            }
        }
        final CoreConfigReportEncoder.AssetsEncoder assets = report.assetsCount(2);
        final String[] codes = { "BTC", "USDTUSDT" };
        for (int e = 1; e <= 2; e++) {
            assets.next().assetId(i(t, e + 4, 1)).code(codes[e - 1]).scale((short) (6 + e));
        }
        print("JAVA_CORE_CONFIG_REPORT", report.encodedLength());

        final BalanceUpdateEncoder update = new BalanceUpdateEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .userId(v(12, 0, 1)).assetId(i(12, 0, 2)).available(v(12, 0, 3)).locked(v(12, 0, 4));
        print("JAVA_BALANCE_UPDATE", update.encodedLength());

        final BalanceUpdateBatchEncoder updates = new BalanceUpdateBatchEncoder().wrapAndApplyHeader(BUF, 0, HEADER);
        final BalanceUpdateBatchEncoder.UpdatesEncoder updateLines = updates.updatesCount(2);
        for (int e = 1; e <= 2; e++) {
            updateLines.next().userId(v(22, e, 1)).assetId(i(22, e, 2)).available(v(22, e, 3)).locked(v(22, e, 4));
        }
        print("JAVA_BALANCE_UPDATE_BATCH", updates.encodedLength());

        final DepositAckEncoder depositAck = new DepositAckEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(15, 0, 1)).userId(v(15, 0, 2)).assetId(i(15, 0, 3)).amount(v(15, 0, 4))
                .newAvailable(v(15, 0, 5)).duplicate(BoolFlag.TRUE);
        print("JAVA_DEPOSIT_ACK", depositAck.encodedLength());

        final DepositRejectEncoder depositReject = new DepositRejectEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(111, 0, 1)).requestIdHigh(v(111, 0, 2)).requestIdLow(v(111, 0, 3))
                .userId(v(111, 0, 4)).assetId(i(111, 0, 5)).amount(v(111, 0, 6))
                .reason(RejectReason.BALANCE_OVERFLOW);
        print("JAVA_DEPOSIT_REJECT", depositReject.encodedLength());

        final WithdrawAckEncoder withdrawAck = new WithdrawAckEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(16, 0, 1)).userId(v(16, 0, 2)).assetId(i(16, 0, 3)).amount(v(16, 0, 4))
                .newAvailable(v(16, 0, 5)).duplicate(BoolFlag.FALSE);
        print("JAVA_WITHDRAW_ACK", withdrawAck.encodedLength());

        final WithdrawRejectEncoder withdrawReject = new WithdrawRejectEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .correlationId(v(14, 0, 1)).userId(v(14, 0, 2)).assetId(i(14, 0, 3)).amount(v(14, 0, 4))
                .reason(RejectReason.REQUEST_ID_MISMATCH).duplicate(BoolFlag.TRUE);
        print("JAVA_WITHDRAW_REJECT", withdrawReject.encodedLength());
    }

    // ---- broadcasts the core sends on the gateway's channels that the gateway ignores ----

    private static void ignored() {
        // CoreEventPublisher.onOrderFault: BROADCAST on CH_CONTROL, which both gateway registrations subscribe to
        final OrderFaultEncoder fault = new OrderFaultEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .kind(OrderFaultKind.FEED_HALT).orderId(v(113, 0, 2)).userId(v(113, 0, 3)).marketId(i(113, 0, 4))
                .tradeId(v(113, 0, 5)).amount(v(113, 0, 6)).logPosition(v(113, 0, 7));
        print("JAVA_ORDER_FAULT", fault.encodedLength());

        // The core opens every session with all the legacy channels until its RegisterSession lands, so these
        // CH_SETTLEMENTS broadcasts can reach a gateway before its mask applies.
        final SettlementAppliedEncoder applied = new SettlementAppliedEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .tradeId(v(13, 0, 1)).buyerUserId(v(13, 0, 2)).sellerUserId(v(13, 0, 3));
        print("JAVA_SETTLEMENT_APPLIED", applied.encodedLength());

        final SettleFaultEncoder settleFault = new SettleFaultEncoder().wrapAndApplyHeader(BUF, 0, HEADER)
                .tradeId(v(21, 0, 1)).orderId(v(21, 0, 2)).userId(v(21, 0, 3)).assetId(i(21, 0, 4))
                .drawnFromAvailable(v(21, 0, 5)).uncovered(v(21, 0, 6));
        print("JAVA_SETTLE_FAULT", settleFault.encodedLength());

        final SettlementAppliedBatchEncoder batch = new SettlementAppliedBatchEncoder().wrapAndApplyHeader(BUF, 0, HEADER);
        final SettlementAppliedBatchEncoder.SettlementsEncoder lines = batch.settlementsCount(2);
        for (int e = 1; e <= 2; e++) {
            lines.next().tradeId(v(24, e, 1)).buyerUserId(v(24, e, 2)).sellerUserId(v(24, e, 3));
        }
        print("JAVA_SETTLEMENT_APPLIED_BATCH", batch.encodedLength());
    }

    public static void main(final String[] args) {
        commands();
        replies();
        ignored();
    }
}
