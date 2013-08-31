/* In order to implement our transition from bare pointers to shared pointers, we need to have
   shared_ptr::release().  http://www.boost.org/doc/libs/1_48_0/libs/smart_ptr/shared_ptr.htm#FAQ
   gives legitimate reasons why we shouldn't be doing this, but for us, all of our extents will
   have a destructor which is just operator new since that's how we make extents.  Therefore we
   do the horrible ugly stuff below in order to implement release and we pray that it all works
   out. This file will of course vanish as soon as we are done with the transition. */

// Give us access to px so that we can null it out.
#define BOOST_NO_MEMBER_TEMPLATE_FRIENDS 1

#include <boost/shared_ptr.hpp>

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/Extent.hpp>

namespace dataseries { namespace hack {
        Extent *releaseExtentSharedPtr(boost::shared_ptr<Extent> &p, Extent *e);
        size_t extentSharedPtrSize();
    }}

class more_evil_hack_1 : public boost::detail::sp_counted_base {
  public:
    Extent *magic;
};

class more_evil_hack_2 {
  public:
    more_evil_hack_1 *evil_1;
};

namespace dataseries { namespace hack {
        Extent *releaseExtentSharedPtr(boost::shared_ptr<Extent> &p, Extent *e) {
            INVARIANT(p.use_count() == 1, "Attempting to convert a shared pointer back to a native"
                      " pointer only works with use count 1; likely you need getExtentShared()");
            Extent *ret = p.px;
            SINVARIANT(ret == e);
            p.px = NULL;
            more_evil_hack_2 *evil = reinterpret_cast<more_evil_hack_2 *>(&p.pn);
            more_evil_hack_1 *more_evil = evil->evil_1;
            SINVARIANT(more_evil->magic == ret);
            more_evil->magic = NULL;
            p.reset();
            try {
                Extent::Ptr p = ret->shared_from_this();
                FATAL_ERROR("how did we get the shared back?");
            } catch (std::exception &) {
                // ok
            }
            return ret;
        }

        size_t extentSharedPtrSize() {
            return sizeof(boost::shared_ptr<Extent>);
        }
    } }
