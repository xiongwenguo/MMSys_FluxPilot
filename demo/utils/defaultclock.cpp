// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#include "defaultclock.hpp"

     DefaultClock* DefaultClock::GetClock()
    {
        static DefaultClock* clock = new DefaultClock();
        return clock;
    }

    // QuicClock implementation.
    QuicTime DefaultClock::ApproximateNow() const
    {
        return Now();
    }

    /// std::steady_clock
    QuicTime DefaultClock::Now() const
    {
        auto nowus= std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now());
        return CreateTimeFromMicroseconds(nowus.time_since_epoch().count());
    }
    /// std::system_clock
    QuicWallTime DefaultClock::WallNow() const
    {
        auto nowms= std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
        return QuicWallTime::FromUNIXMicroseconds(nowms.time_since_epoch().count());
    }
    QuicTime DefaultClock::ConvertWallTimeToQuicTime(
            const QuicWallTime& walltime) const
    {
        return CreateTimeFromMicroseconds(walltime.ToUNIXMicroseconds());
    }

