// -*- C++ -*-
//===-------------------------- algorithm ---------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

/*

==============================================================================
LLVM Release License
==============================================================================
University of Illinois/NCSA
Open Source License

Copyright (c) 2003-2010 University of Illinois at Urbana-Champaign.
All rights reserved.

Developed by:

    LLVM Team

    University of Illinois at Urbana-Champaign

    http://llvm.org

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal with
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimers.

    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimers in the
      documentation and/or other materials provided with the distribution.

    * Neither the names of the LLVM Team, University of Illinois at
      Urbana-Champaign, nor the names of its contributors may be used to
      endorse or promote products derived from this Software without specific
      prior written permission.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE
SOFTWARE.

*/

// Inlined copy of std::lower_bound, with an additional check added
// for the impossible situation where the length of the list being
// searched goes negative.

template <class _Compare, class _ForwardIterator, class _Tp>
_ForwardIterator
__lower_bound1(_ForwardIterator __first, _ForwardIterator __last, const _Tp &__value_, _Compare __comp) {
	typedef typename std::iterator_traits<_ForwardIterator>::difference_type difference_type;
	difference_type __len = std::distance(__first, __last);
	while (__len > 0) {
		difference_type __l2 = __len / 2;
		_ForwardIterator __m = __first;
		std::advance(__m, __l2);
		if (__comp(*__m, __value_)) {
			__first = ++__m;
			__len -= __l2 + 1;
		} else
			__len = __l2;
	}
	if (__len < 0) {
		fprintf(stderr, "Error: Input file is out of sort\n");
		exit(EXIT_FAILURE);
	}
	return __first;
}

template <class _ForwardIterator, class _Tp, class _Compare>
inline __attribute__((__visibility__("hidden"), __always_inline__))
_ForwardIterator
lower_bound1(_ForwardIterator __first, _ForwardIterator __last, const _Tp &__value_, _Compare __comp) {
	typedef typename std::add_lvalue_reference<_Compare>::type _Comp_ref;
	return __lower_bound1<_Comp_ref>(__first, __last, __value_, __comp);
}

template <class _ForwardIterator, class _Tp>
inline __attribute__((__visibility__("hidden"), __always_inline__))
_ForwardIterator
lower_bound1(_ForwardIterator __first, _ForwardIterator __last, const _Tp &__value_) {
	return lower_bound1(__first, __last, __value_,
			    std::less<typename std::iterator_traits<_ForwardIterator>::value_type>());
}
