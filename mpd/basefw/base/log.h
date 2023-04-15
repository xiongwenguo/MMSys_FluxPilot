// Copyright (c) 2023. ByteDance Inc. All rights reserved.
#pragma once
#include "basefw/commdef.h"

#ifndef SPDLOG_ACTIVE_LEVEL
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#include "spdlog/spdlog.h"
#include "spdlog/fmt/bundled/ranges.h" // for print std::vector
// see url below for print function line file info
//https://github.com/gabime/spdlog/issues/1032#issuecomment-475237476

#endif



