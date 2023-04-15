/*******************************************************
*
* Author(s): Pxf
*
*******************************************************/
#ifndef _BASEFW_SHARED_PTR_H_
#define _BASEFW_SHARED_PTR_H_

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

namespace basefw
{
    // noncopyable
    typedef boost::noncopyable noncopyable;

    // shared_ptr/weak_ptr/enable_shared_from_this
    // c++11 Alias template
    template <typename T>
    using shared_ptr = std::shared_ptr<T>;

    template <typename T>
    using weak_ptr = std::weak_ptr<T>;

    template <typename T>
    using enable_shared_from_this = std::enable_shared_from_this<T>;

    template<class T, class U> shared_ptr<T> static_pointer_cast( shared_ptr<U> const & r ) noexcept
    {
        return std::static_pointer_cast<T>(r);
    }

    template<class T, class U> shared_ptr<T> dynamic_pointer_cast( shared_ptr<U> const & r ) noexcept
    {
        return std::dynamic_pointer_cast<T>(r);
    }

}

#endif