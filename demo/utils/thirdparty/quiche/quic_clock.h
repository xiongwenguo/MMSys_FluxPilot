// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
///Modified 2023. ByteDance Inc.

#pragma once

#include "quic_time.h"

/* API_DESCRIPTION
 QuicClock is used by QUIC core to get current time. Its instance is created by
 applications and passed into QuicDispatcher and QuicConnectionHelperInterface.
 API-DESCRIPTION */
namespace basefw
{
    namespace quic
    {

// Interface for retrieving the current time.
        class QuicClock
        {
        public:
            QuicClock();

            virtual ~QuicClock() = default;

            QuicClock(const QuicClock&) = delete;

            QuicClock& operator=(const QuicClock&) = delete;

            // Returns the approximate current time as a QuicTime object.
            virtual QuicTime ApproximateNow() const = 0;

            // Returns the current time as a QuicTime object.
            // Note: this use significant resources please use only if needed.
            virtual QuicTime Now() const = 0;

            // WallNow returns the current wall-time - a time that is consistent across
            // different clocks.
            virtual QuicWallTime WallNow() const = 0;

            // Converts |walltime| to a QuicTime relative to this clock's epoch.
            virtual QuicTime ConvertWallTimeToQuicTime(
                    const QuicWallTime& walltime) const = 0;

            // Creates a new QuicTime using |time_us| as the internal value.
            virtual QuicTime CreateTimeFromMicroseconds(uint64_t time_us) const;
        };

    }  // namespace quic
}


