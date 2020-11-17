# Copyright (c) 2019, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
from functools import wraps
from typing import Callable


def analysis(fn):
    """Mark a web route as requiring OAuth sign-in."""

    @wraps(fn)
    def decorated_function(events_url, tree_name, accumulator, *args, **kwargs):
        import awkward1 as ak
        from coffea.nanoevents import NanoEventsFactory
        from coffea.nanoevents import BaseSchema

        # This in is amazingly important - the invar mass will fail silently without it.
        # And must be done in here as this function is shipped off to the funcx processor
        # on a remote machine/remote python environment.
        from coffea.nanoevents.methods import candidate
        ak.behavior.update(candidate.behavior)

        # Use NanoEvents to build a 4-vector
        events = NanoEventsFactory.from_file(
            file=str(events_url),
            treepath=f'/{tree_name}',
            schemaclass=BaseSchema,
            metadata={
                'dataset': 'mc15x',
                'filename': str(events_url)
            }
        ).events()

        print("Events ", events)
        return fn(events=events, events_url=events_url, tree_name=tree_name,
                  accumulator=accumulator, *args, **kwargs)

    return decorated_function


class Analysis:
    def __init__(self):
        pass
