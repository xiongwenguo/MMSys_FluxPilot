// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#include "quic_clock.h"
namespace basefw
{
    namespace quic
    {
        QuicClock::QuicClock()
        {

        }

        QuicTime QuicClock::CreateTimeFromMicroseconds(uint64_t time_us) const
        {
            return QuicTime(time_us);
        }
    }
}
