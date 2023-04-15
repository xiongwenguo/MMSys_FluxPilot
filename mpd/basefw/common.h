// Copyright (c) 2023. ByteDance Inc. All rights reserved.
/*******************************************************
 * 
 * Author(s): Pxf
 * 
 *******************************************************/
#ifndef _BASEFW_COMMON_H_
#define _BASEFW_COMMON_H_

#ifdef BASE_WIN_PLAT

#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0501
#endif

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#ifndef NOMINMAX
#define NOMINMAX 1
#endif

#pragma warning(disable:4996)
#include <Windows.h>

#else

#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

namespace basefw
{

}

namespace fw = basefw;

#define BASE_LOGGER "base"


#endif
