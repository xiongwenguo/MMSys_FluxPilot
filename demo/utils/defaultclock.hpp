// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once


#include "demo/utils/thirdparty/quiche/quic_clock.h"
#include <chrono>

using basefw::quic::QuicClock;
using basefw::quic::QuicTime;
using basefw::quic::QuicWallTime;

/// use std::chrono::steady timer implement the QuicClock Interface
class DefaultClock : public QuicClock
{
public:
    static DefaultClock* GetClock();

    explicit DefaultClock() = default;

    ~DefaultClock() override = default;

    DefaultClock(const DefaultClock&) = delete;

    DefaultClock& operator=(const DefaultClock&) = delete;

    // QuicClock implementation.
    QuicTime ApproximateNow() const override;

    /// std::steady_clock
    QuicTime Now() const override;

    /// std::system_clock
    QuicWallTime WallNow() const override;

    QuicTime ConvertWallTimeToQuicTime(
            const QuicWallTime& walltime) const override;
};
