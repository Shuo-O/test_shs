#pragma once

#include <vector>
#include <string>
#include <iostream>

#ifdef _MSC_VER
#undef max
#undef min
#endif

struct alignas(8) MDUniOrder {
    int8_t type = 1;
    int8_t bsFlag = 0; 
    int8_t tickType = 0;
    int16_t channel = 0;
    int32_t instrumentId = 0;  //股票代码
    int32_t nTP = 0;
    int32_t recvTime = 0; 
    int32_t price = 0;
    int32_t qty = 0;
    int32_t bizSeq = 0;
    uint32_t orderSeq = 0;
    uint32_t orderId = 0;
    //int32_t _pad[2];
};