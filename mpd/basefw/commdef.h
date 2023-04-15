// Copyright (c) 2023. ByteDance Inc. All rights reserved.
/*******************************************************
*
* Author(s): Pxf
*
*******************************************************/
#ifndef _BASEFW_COMMDEF_H_
#define _BASEFW_COMMDEF_H_

#include "basefw/common.h"

#if defined(_MSC_VER)
#define __STR2__(x) #x
#define __STR1__(x) __STR2__(x)
#define __LOC__ __FILE__ "("__STR1__(__LINE__)") : "
#define _MSG_PRAGMA(_msg) __pragma(message (__LOC__ _msg))
#elif defined(__GNUC__)
#define _DO_PRAGMA(x) _Pragma (#x)
#define _MSG_PRAGMA(_msg) _DO_PRAGMA(message (_msg))
#else
#define _MSG_PRAGMA(_msg) 
#endif

#define NOTIFY(x) _MSG_PRAGMA("Notify : " #x)

#endif
