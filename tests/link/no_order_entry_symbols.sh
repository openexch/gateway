#!/bin/sh
# The finished oe-stream (or oe-projector, built the same way) holds no symbol of an order-entry or money-command
# encoder; oe-gateway does (the control: the same pattern finds them there, so an empty answer means something).
# usage: no_order_entry_symbols.sh <oe-stream | oe-projector> <oe-gateway>
set -eu
stream="$1"
gateway="$2"
pattern='openexchange::gateway::wire::encode\(openexchange::gateway::wire::(CreateOrder|CancelOrder|AmendOrder) const|openexchange::gateway::wire::encode(Deposit|Withdraw)\(|openexchange::gateway::session::encodeCommand\('
if ! nm -C "$gateway" | grep -Eq "$pattern"; then
    echo "control failed: no order-entry encoder symbol in $gateway (the check would see nothing)"
    exit 1
fi
if ! nm -C "$stream" | grep -q 'openexchange::gateway::session::encodeReadCommand('; then
    echo "no symbols to check in $stream"
    exit 1
fi
if nm -C "$stream" | grep -E "$pattern"; then
    echo "FAIL $stream holds an order-entry encoder"
    exit 1
fi
echo "PASS $stream holds no order-entry encoder ($gateway does)"
