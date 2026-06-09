#pragma once

// MDUniOrder is the problem-supplied wire type for DemoMd::on_md. It is part of
// the task interface, not the implementation, so v2 uses the exact same 40-byte
// ABI. Do not change field order or sizes: the on_md signature depends on it.

#include <cstdint>

struct alignas(8) MDUniOrder {
    int8_t   type = 1;
    int8_t   bsFlag = 0;
    int8_t   tickType = 0;
    int16_t  channel = 0;
    int32_t  instrumentId = 0;  // A-share security code
    int32_t  nTP = 0;
    int32_t  recvTime = 0;      // hhmmss-style integer time
    int32_t  price = 0;
    int32_t  qty = 0;
    int32_t  bizSeq = 0;
    uint32_t orderSeq = 0;
    uint32_t orderId = 0;
};

static_assert(sizeof(MDUniOrder) == 40, "MDUniOrder must stay 40 bytes");
