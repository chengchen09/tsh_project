/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2011 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the GNU General Public License for the complete license terms.
*
* You should have received a copy of the GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
*
* END_COPYRIGHT
*/

/**
 * @file Singleton.h
 *
 * @brief Helper template for creating global objects
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef SINGLETON_H_
#define SINGLETON_H_

#include "util/Mutex.h"

namespace scidb
{

/**
 * @brief Singleton template
 *
 * Global objects must be derived from this template. For example:
 * @code
 * class MyGlobalObject: public Singleton<MyGlobalObject>
 * {
 * public:
 *   void myMethod();
 * ...
 * }
 * @endcode
 *
 * After this you can access to your global object like this:
 * @code
 * MyGlobalObject::getInstance()->myMethod();
 * @endcode
 */
template<typename Derived>
class Singleton
{
public:
	/**
	 * Construct and return derived class object
	 * @return pointer to constructed object
	 */
	static Derived* getInstance()
	{
		// Double checking instance for avoiding locking overhead (see DLCP)
		if (!_instance)   // Maybe better to avoid this checking
		{
			ScopedMutexLock mutexLock(_instance_mutex);
			if (!_instance)
			{
				// Initializing volatile temp variable before for preventing
				// reordering data by compiler on some platforms (instance can
				// be assigned early then fully initialized).
			    // Note: There are some problems. See:
			    // http://en.wikipedia.org/wiki/Memory_barrier
			    // http://en.wikipedia.org/wiki/Double_checked_locking
				Derived* volatile temp = new Derived();
				_instance = temp;
			}
		}
		return _instance;
	}

	/**
	 * Destroy global object
	 */
	static void destroy()
	{
		if (_instance)
		{
            ScopedMutexLock mutexLock(_instance_mutex);
			if (_instance)
			{
				delete _instance;
				_instance = NULL;
			}
		}
	}

protected:
	Singleton() {};
	virtual ~Singleton() {};

private:
	static Derived* volatile _instance;
	static Mutex _instance_mutex;
};

template <typename Derived>
Derived* volatile Singleton<Derived>::_instance = NULL;

template <typename Derived>
Mutex Singleton<Derived>::_instance_mutex;

}

#endif /* SINGLETON_H_ */
